# rosbag2_to_video command line tool

## Installation

The only method available is from source.
It can be added to your colcon workspace and be installed as usual.

```bash
# create workspace
mkdir -p new_ws/src
cd new_ws/src
# clone repo
git clone https://github.com/ivanpauno/rosbag2_to_video
cd ..
# install dependencies
rosdep install --from-paths src --ignore-src -y
# build workspace
colcon build
```

## Usage

The tool has two entrypoints:

```bash
ros2 bag to_video
```

or

```bash
ros2 run rosbag2_to_video rosbag2_to_video
```

Use `--help` to see options.
