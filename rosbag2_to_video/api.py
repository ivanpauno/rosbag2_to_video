# Copyright 2022 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
from typing import Any
from typing import Callable
from typing import Tuple
from typing import TYPE_CHECKING

import cv2
from cv_bridge import CvBridge

from rclpy.serialization import deserialize_message
import rclpy.time
import rosbag2_py
from rosidl_runtime_py.utilities import get_message

if TYPE_CHECKING:
    from argparse import ArgumentParser
    import numpy as np


def cv2_video_writer_fourcc(codec_str: str) -> int:
    """Validate the user provided video codec string and return it as fourcc."""
    if len(codec_str) != 4:
        raise ValueError(f'codecs should be specified using fourcc, got "{codec_str}"')
    try:
        return cv2.VideoWriter.fourcc(*codec_str)
    except Exception:
        raise ValueError(f'"{codec_str}" is not a valid fourcc codec')


def add_arguments_to_parser(argparser: 'ArgumentParser'):
    """Define command line arguments for the bag to video tool."""
    argparser.add_argument(
        'bagfile',
        help='Path to the bag'
    )
    argparser.add_argument(
        '-t',
        '--topic',
        required=True,
        help=(
            'Name of the image topic (currently only supports sensor_msgs/msg/Image or '
            'sensor_msgs/msg/CompressedImage types)')
    )
    argparser.add_argument(
        '-o',
        '--output',
        required=True,
        help='Output filename (currently only supports MP4)'
    )
    argparser.add_argument('--fps', type=float, required=True, help='Output frames per second')
    argparser.add_argument(
        '--storage-id',
        type=str,
        default='sqlite3',
        help='Rosbag2 storage id. If a bag folder is provided, this is ignored.')
    argparser.add_argument(
        '--codec', type=cv2_video_writer_fourcc, default="avc1", help='Video codec')


class CommandInputError(ValueError):
    """Error raised when provided command line arguments are not valid."""

    def __init__(self, msg: str):
        super().__init__(msg)


def get_stamp_from_image_msg(image_msg) -> float:
    """Convert timestamp in msg from nanoseconds to seconds."""
    stamp = rclpy.time.Time.from_msg(image_msg.header.stamp).nanoseconds
    stamp = stamp / 1e9
    return stamp


def get_topic_type(topic_name: str, topics_and_types) -> str:
    """Get the topic type from the topic name and the topic information in the bag."""
    try:
        topic_type = next(x for x in topics_and_types if x.name == topic_name).type
    except StopIteration:
        raise CommandInputError(
            f'Topic {topic_name} was not recorded in the bagfile')
        return None
    if topic_type not in ('sensor_msgs/msg/Image', 'sensor_msgs/msg/CompressedImage'):
        raise CommandInputError(
            'topic type should be sensor_msgs/msg/Image or '
            f'sensor_msgs/msg/CompressedImage, got {topic_type}')
    return topic_type


class SequentialImageBagReader:
    """Reader of images from a bagfile source sequentially"""

    def __init__(self, bag_reader: rosbag2_py.SequentialReader, topic_name: str):
        """
        Create image bagfile reader.

        :param bag_reader: rosbag2_py sequential reader instance.
        :param topic_name: topic from where to read the images in the bagfile.
        """
        self._bag_reader: rosbag2_py.SequentialReader = bag_reader
        self._cvbridge: CvBridge = CvBridge()
        self._topic_name: str = topic_name
        self._topic_type: str = get_topic_type(topic_name, bag_reader.get_all_topics_and_types())
        self._msg_type = get_message(self._topic_type)
        self._msg_to_cv2: Callable[[Any], 'np.ndarray']
        if self._topic_type == 'sensor_msgs/msg/Image':
            self._msg_to_cv2 = lambda msg: self._cvbridge.imgmsg_to_cv2(msg, 'bgr8')
        elif self._topic_type == 'sensor_msgs/msg/CompressedImage':
            self._msg_to_cv2 = lambda msg: self._cvbridge.compressed_imgmsg_to_cv2(msg, 'bgr8')
        storage_filter = rosbag2_py.StorageFilter(topics=[topic_name])
        self._bag_reader.set_filter(storage_filter)
    
    def next(self) -> Tuple['np.ndarray', float]:
        """Return next image and its timestamp."""
        _, data, _ = self._bag_reader.read_next()    
        image_msg = deserialize_message(data, self._msg_type)
        return self._msg_to_cv2(image_msg), get_stamp_from_image_msg(image_msg)
    
    def has_next(self):
        """Return true if there is at least one more message to read."""
        return self._bag_reader.has_next()


class SequentialVideoWriter:
    """Create videos from images provided sequentially."""

    def __init__(
        self,
        cv_video_writer: cv2.VideoWriter,
        first_cv_image: 'np.ndarray',
        start_stamp: float,
        fps: float,
    ):
        """
        Create a video writer.

        :param cv_video_writer: A cv2.VideoWriter instance, already opened.
        :param first_cv_image: First image to write in the video.
        :param start_stamp: Timestamp of the first image.
        :param fps: Fps used to record the video.
        """
        self._fps: float = fps
        self._cv_video_writer: cv2.VideoWriter = cv_video_writer
        self._cv_video_writer.write(first_cv_image)
        self._images_processed: int = 1
        self._frame_count: int = 1
        self._images_skipped: int = 0
        self._start_stamp: float = start_stamp
        self._last_image: 'np.ndarray' = first_cv_image
        self._last_image_written_once: bool = True
    
    def add_frame(self, cv_image: 'np.ndarray', stamp: float):
        """Add a frame to the video, given an image and its timestamp."""

        self._images_processed += 1
        t_from_start = stamp - self._start_stamp

        # accept jitter up to 0.5/fps
        if t_from_start < (float(self._frame_count) - 0.5) / self._fps:
            self._last_image = cv_image
            if not self._last_image_written_once:
                self._images_skipped += 1
                print(
                    'video fps is too low compared to image publish rate, skipping one message',
                    file=sys.stderr)
            self._last_image_written_once = False
            return
        current_image_frame_index = int(round(t_from_start * self._fps))
        repeat_last_image = current_image_frame_index - self._frame_count
        for _ in range(repeat_last_image):
            self._cv_video_writer.write(self._last_image)
        self._cv_video_writer.write(cv_image)
        self._last_image = cv_image
        if not self._last_image_written_once and repeat_last_image == 0:
            self._images_skipped += 1
            print(
                'video fps is too low compared to image publish rate, skipping one message',
                file=sys.stderr)
        self._last_image_written_once = True
        self._frame_count += repeat_last_image + 1
    
    def close(self):
        """Close the video writer."""
        if not self._last_image_written_once:
            self._cv_video_writer.write(self._last_image)
        self._cv_video_writer.release()
    
    @property
    def images_processed(self) -> int:
        """
        Get the number of images that were provided.

        Images that were skipped and not written are also counted.
        """
        return self._images_processed

    @property
    def frames_written(self) -> int:
        """
        Number of frames written to the video.
        """
        return self._frame_count
    
    @property
    def images_skipped(self) -> int:
        """Number of images that were skipped and not written to the video."""
        return self._images_skipped


def create_sequential_image_bag_reader(
    bag_path: str, storage_id: str, topic_name: str
) -> SequentialImageBagReader:
    """
    Create a SequentialImageBagReader instance.

    :param bag_path: Path to the bagfile (folder with metadata or file).
    :param storage_id: Storage id of the bagfile.
    :param topic_name: Name of the image topic.
    """
    if os.path.isdir(bag_path):
        # load storage id from metadata.yaml
        storage_options = rosbag2_py.StorageOptions(uri=bag_path)
    else:
        storage_options = rosbag2_py.StorageOptions(uri=bag_path, storage_id=storage_id)
    # TODO(jacobperron): Shouldn't we be able to infer serialization format from metadaya.yaml?
    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format='cdr', output_serialization_format='cdr')
    bag_reader = rosbag2_py.SequentialReader()
    bag_reader.open(storage_options, converter_options)

    if not bag_reader.has_next():
        raise CommandInputError('empty bag file')
    return SequentialImageBagReader(bag_reader, topic_name)


def create_sequential_video_writer(
    output_path: str,
    codec: int,
    fps: float,
    first_cv_image: 'np.ndarray',
    start_stamp: float
):
    """
    Create a SequentialVideoWriter instance.

    :param output_path: Path of the video to be created.
    :param codec: fourcc of the codec to be used.
    :param fps: Video frame per second to be used.
    :param first_cv_image: First image to write to the video.
        Video width and height are got from here.
    :param start_stamp: Timestamp of the first image.
    """

    height, width, _ = first_cv_image.shape
    cv_video_writer = cv2.VideoWriter()
    success = cv_video_writer.open(
        output_path,
        cv2.CAP_FFMPEG,
        codec,
        fps,
        (width, height))
    if not success:
        raise CommandInputError(f'Failed to open file {output_path}')
    return SequentialVideoWriter(cv_video_writer, first_cv_image, start_stamp, fps)


def convert_bag_to_video(
    bag_path: str, storage_id: str, topic_name: str, output_path: str, codec: int, fps: float
):
    """Create a bagfile from a video."""
    # Force the .mp4 extension on file output name
    if output_path[-4:] != '.mp4':
        output_path += '.mp4'
    image_reader = create_sequential_image_bag_reader(bag_path, storage_id, topic_name)
    cv_image, start_stamp = image_reader.next()
    video_writer = create_sequential_video_writer(output_path, codec, fps, cv_image, start_stamp)

    while image_reader.has_next():
        cv_image, stamp = image_reader.next()
        video_writer.add_frame(cv_image, stamp)
    video_writer.close()

    print(
        f'Processed {video_writer.images_processed} messages and wrote '
        f'{video_writer.frames_written} frames. '
        f'{video_writer.images_skipped} messages were skipped',
        file=sys.stderr)
    print(f'Output video: {output_path}', file=sys.stderr)


def main(args):
    """
    Create a bagfile from a video.

    Wrapper of convert_bag_to_video(), that handles exceptions and prints errors instead.
    """
    try:
        convert_bag_to_video(
            args.bagfile, args.storage_id, args.topic, args.output, args.codec, args.fps)
    except CommandInputError as e:
        print(e, file=sys.stderr)
    except Exception as e:
        print(f'Unexpected exception of type [{type(e)}]: {e}', file=sys.stderr)
