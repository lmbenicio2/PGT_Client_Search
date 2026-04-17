import pyrealsense2 as rs
import cv2
import numpy as np
import os
from datetime import datetime

# Path to the main folder containing subfolders with .bag files
main_folder_path = r"G:\Angus_ID\I-Farms-Growth_Tracking\Round_08\10-05-23\ORIGINAL_DATA"
output_main_path = r"G:\Angus_ID\I-Farms-Growth_Tracking\Round_08\10-05-23\EXPORTED_FRAMES"

# Function to process each .bag file
def process_bag_file(bag_file_path, output_folder_path):
    # Configure pipeline settings
    pipeline = rs.pipeline()
    config = rs.config()
    rs.config.enable_device_from_file(config, bag_file_path, repeat_playback=False)

    # Start the pipeline
    pipeline_profile = pipeline.start(config)

    # Create align object
    align_to = rs.stream.color
    align = rs.align(align_to)

    # Get playback device
    playback = pipeline_profile.get_device().as_playback()

    # Create a specific output subdirectory for the .bag file
    bag_filename = os.path.splitext(os.path.basename(bag_file_path))[0]
    bag_output_folder_path = os.path.join(output_folder_path, bag_filename)
    os.makedirs(bag_output_folder_path, exist_ok=True)

    # Create subfolders for color and depth images
    color_output_folder_path = os.path.join(bag_output_folder_path, "COLOR")
    depth_output_folder_path = os.path.join(bag_output_folder_path, "DEPTH")
    os.makedirs(color_output_folder_path, exist_ok=True)
    os.makedirs(depth_output_folder_path, exist_ok=True)

    frame_count = 0  # Variable to track the frame count
    timeout_ms = 10000
    try:
        while True:
            # Wait for frames
            frames = pipeline.wait_for_frames(timeout_ms)

            # Align frames
            aligned_frames = align.process(frames)

            # Get aligned color and depth frames
            color_frame = aligned_frames.get_color_frame()
            depth_frame = aligned_frames.get_depth_frame()

            # Check if frames are valid
            if not color_frame or not depth_frame:
                # Assume no more frames to process if either frame is None
                print("No more frames to process.")
                break

            # Convert frames to numpy arrays
            color_image = np.asanyarray(color_frame.get_data())
            depth_image = np.asanyarray(depth_frame.get_data())

            # Convert color image to BGR color space (or the desired color space)
            color_image_bgr = cv2.cvtColor(color_image, cv2.COLOR_RGB2BGR)

            # Extract timestamp
            timestamp = frames.get_timestamp()
            timestamp_str = datetime.fromtimestamp(timestamp / 1000).strftime('%H_%M')

            # Save color image as JPG
            color_filename = os.path.join(color_output_folder_path, f"{bag_filename}_color_{timestamp_str}_{frame_count}.jpg")
            cv2.imwrite(color_filename, color_image_bgr)

            # Save depth image as PNG (or any other format)
            depth_filename = os.path.join(depth_output_folder_path, f"{bag_filename}_depth_{timestamp_str}_{frame_count}.png")
            cv2.imwrite(depth_filename, depth_image)

            print(f"Frame {frame_count} in {os.path.basename(bag_file_path)}: Color image and depth image saved with timestamp.")
            frame_count += 1  # Increment the frame count

            # Check if playback has finished
            playback_status = playback.current_status()
            if playback_status == rs.playback_status.stopped:
                print("Playback complete.")
                break

    except Exception as e:
        print(f"An error occurred: {e}")

    # Stop the pipeline after processing each bag file
    pipeline.stop()

# Check for .bag files in the main folder and subfolders
def check_and_process_bag_files(folder_path, output_folder_path):
    bag_files = [file for file in os.listdir(folder_path) if file.endswith(".bag")]
    if bag_files:
        # Process .bag files in the main folder
        for bag_file in bag_files:
            bag_file_path = os.path.join(folder_path, bag_file)
            process_bag_file(bag_file_path, output_folder_path)
    else:
        # Loop through each subfolder in the main folder
        for subfolder in os.listdir(folder_path):
            subfolder_path = os.path.join(folder_path, subfolder)
            if os.path.isdir(subfolder_path):
                # Create corresponding output subfolder
                output_subfolder_path = os.path.join(output_folder_path, subfolder)
                os.makedirs(output_subfolder_path, exist_ok=True)
                check_and_process_bag_files(subfolder_path, output_subfolder_path)

# Start processing
check_and_process_bag_files(main_folder_path, output_main_path)

