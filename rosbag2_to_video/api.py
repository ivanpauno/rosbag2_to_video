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

import rosbag2_py
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message
import rclpy.time

import cv2
from cv_bridge import CvBridge


def get_stamp_from_image_msg(image_msg):
    # nanoseconds to seconds
    stamp = rclpy.time.Time.from_msg(image_msg.header.stamp).nanoseconds
    stamp = stamp / 1e9
    return stamp


def add_arguments_to_parser(argparser):
    argparser.add_argument(
        'bagfile', help='Path to the bag (currently only supports sensor_msgs/msg/Image types)')
    argparser.add_argument('-t','--topic', required=True, help='Name of the image topic')
    argparser.add_argument('-o','--output', required=True, help='Output filename (currently only supports MP4)')
    argparser.add_argument('--fps', type=int, required=True, help='Output frames per second')


def convert_bag_to_video(args):
    # Force the .mp4 extension on file output name
    if args.output[-4:] != '.mp4':
        args.output += '.mp4'

    storage_options = rosbag2_py.StorageOptions(uri=args.bagfile)
    # TODO(jacobperron): Shouldn't we be able to infer serialization format from metadaya.yaml?
    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format='cdr', output_serialization_format='cdr')

    bag_reader = rosbag2_py.SequentialReader()
    bag_reader.open(storage_options, converter_options)

    storage_filter = rosbag2_py.StorageFilter(topics=[args.topic])
    bag_reader.set_filter(storage_filter)

    cvbridge = CvBridge()
    video_writer = cv2.VideoWriter()

    if not bag_reader.has_next():
        print('empty bag file')
        return
    msg_type = get_message('sensor_msgs/msg/Image')

    _, data, _ = bag_reader.read_next()
    image_msg = deserialize_message(data, msg_type)
    # cv2.VideoWriter expects BGR encoding 
    cv_image = cvbridge.imgmsg_to_cv2(image_msg, 'bgr8')
    height, width, _ = cv_image.shape
    success = video_writer.open(
        args.output,
        cv2.CAP_FFMPEG,
        cv2.VideoWriter.fourcc('a', 'v', 'c', '1'),
        args.fps,
        (width, height))
    if not success:
        print(f'Failed to open file {args.output}')
        return
    video_writer.write(cv_image)

    # loop that writes frame
    # each iteration will write the previous image repeatedly
    # based on the fps, start stamp, and the timestamp of the new image.
    msg_count = 1
    frame_count = 1
    skipped_msgs = 0
    start_stamp = get_stamp_from_image_msg(image_msg)
    prev_image = cv_image
    prev_image_written_once = True
    while bag_reader.has_next():
        msg_count += 1
        _, data, _ = bag_reader.read_next()
        image_msg = deserialize_message(data, msg_type)
        # cv2.VideoWriter expects BGR encoding 
        cv_image = cvbridge.imgmsg_to_cv2(image_msg, 'bgr8')

        # nanoseconds to seconds
        stamp = get_stamp_from_image_msg(image_msg)
        t_from_start = stamp - start_stamp

        # accept jitter up to 0.5/fps
        if t_from_start < (float(frame_count) - 0.5) / args.fps:
            prev_image = cv_image
            if not prev_image_written_once:
                skipped_msgs += 1
                print('video fps is too low compared to image publish rate, skipping one message')
            prev_image_written_once = False
            continue
        current_image_frame_index = int(round(t_from_start * args.fps))
        repeat_prev_image = current_image_frame_index - frame_count
        for _ in range(repeat_prev_image):
            video_writer.write(prev_image)
        video_writer.write(cv_image)
        prev_image = cv_image
        if not prev_image_written_once and repeat_prev_image == 0:
            skipped_msgs += 1
            print('video fps is too low compared to image publish rate, skipping one message')
        prev_image_written_once = True
        frame_count += repeat_prev_image + 1

    if not prev_image_written_once:
        skipped_msgs += 1
        print('video fps is too low compared to image publish rate, skipping one message')
    if video_writer is not None:
        video_writer.release()

    print(
        f'Processed {msg_count} message and wrote {frame_count} frames.'
        f' {skipped_msgs} messages were skipped')
    if success:
        print(f'Output video: {args.output}')
