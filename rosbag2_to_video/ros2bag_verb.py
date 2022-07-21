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

from ros2bag.verb import VerbExtension

from . import api


class ToVideo(VerbExtension):
    """Convert a ROS 2 bag into a video."""

    def add_arguments(self, parser, cli_name):
        api.add_arguments_to_parser(parser)

    def main(self, *, args):
        api.convert_bag_to_video(args)
