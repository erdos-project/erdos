from itertools import combinations
import numpy as np
from numpy.linalg import inv
from open3d import draw_geometries, read_point_cloud
import PIL.ImageDraw as ImageDraw
import PIL.ImageFont as ImageFont

from carla.image_converter import depth_to_local_point_cloud
from carla.transform import Transform


def add_bounding_box_text(draw, xmin, ymax, ymin, color, texts):
    try:
        font = ImageFont.truetype('arial.ttf', 24)
    except IOError:
        font = ImageFont.load_default()
    # Check if there's space to add text at the top of the box.
    text_str_heights = [font.getsize(text)[1] for text in texts]
    # Each display_str has a top and bottom margin of 0.05x.
    total_text_str_height = (1 + 2 * 0.05) * sum(text_str_heights)

    if ymax > total_text_str_height:
        text_bottom = ymax
    else:
        text_bottom = ymin + total_text_str_height
    # Reverse list and print from bottom to top.
    for text in texts[::-1]:
        text_width, text_height = font.getsize(text)
        margin = np.ceil(0.05 * text_height)
        draw.rectangle(
            [(xmin, text_bottom - text_height - 2 * margin),
             (xmin + text_width, text_bottom)],
            fill=color)
        draw.text(
            (xmin + margin, text_bottom - text_height - margin),
            text, fill='black', font=font)
        text_bottom -= text_height - 2 * margin


def add_bounding_box(image, corners, color='red', thickness=4, texts=None):
    draw = ImageDraw.Draw(image)
    (xmin, xmax, ymin, ymax) = corners
    draw.line([(xmin, ymax), (xmin, ymin), (xmax, ymin),
               (xmax, ymax), (xmin, ymax)], width=thickness, fill=color)
    if texts:
        add_bounding_box_text(draw, xmin, ymax, ymin, color, texts)


def map_ground_3D_transform_to_2D(image,
                                  world_transform,
                                  rgb_transform,
                                  rgb_intrinsic,
                                  rgb_img_size,
                                  obstacle_transform):
    extrinsic_mat = world_transform * rgb_transform
    pos = obstacle_transform.to_transform_pb2().location
    pos_vector = np.array([[pos.x], [pos.y], [pos.z], [1.0]])
    transformed_3d_pos = np.dot(inv(extrinsic_mat.matrix),
                                pos_vector)
    pos2d = np.dot(rgb_intrinsic, transformed_3d_pos[:3])
    pos2d = np.array([
        pos2d[0] / pos2d[2],
        pos2d[1] / pos2d[2],
        pos2d[2]])
    (img_width, img_height) = rgb_img_size
    if pos2d[2] > 0:
        x_2d = img_width - pos2d[0]
        y_2d = img_height - pos2d[1]
        if (x_2d >= 0 and x_2d < img_width and y_2d >= 0 and y_2d < img_height):
            return (x_2d, y_2d, pos2d[2])
    return None


def map_ground_bounding_box_to_2D(image,
                                  distance_img,
                                  world_transform,
                                  obstacle_transform,
                                  bounding_box,
                                  rgb_transform,
                                  rgb_intrinsic,
                                  rgb_img_size):
    (image_width, image_height) = rgb_img_size
    extrinsic_mat = world_transform * rgb_transform
    obj_transform = Transform(obstacle_transform.to_transform_pb2())
    bbox_pb2 = bounding_box.to_bounding_box_pb2()
    bbox_transform = Transform(bbox_pb2.transform)
    ext = bbox_pb2.extent

    # 8 bounding box vertices relative to (0,0,0)
    bbox = np.array([
        [  ext.x,   ext.y,   ext.z],
        [  ext.x, - ext.y,   ext.z],
        [  ext.x,   ext.y, - ext.z],
        [  ext.x, - ext.y, - ext.z],
        [- ext.x,   ext.y,   ext.z],
        [- ext.x, - ext.y,   ext.z],
        [- ext.x,   ext.y, - ext.z],
        [- ext.x, - ext.y, - ext.z]
    ])

    # Transform the vertices with respect to the bounding box transform.
    bbox = bbox_transform.transform_points(bbox)

    # The bounding box transform is with respect to the object transform.
    # Transform the points relative to its transform.
    bbox = obj_transform.transform_points(bbox)

    # Object's transform is relative to the world. Thus, the bbox contains
    # the 3D bounding box vertices relative to the world.

    coords = []
    for vertex in bbox:
        pos_vector = np.array([
            [vertex[0,0]],  # [[X,
            [vertex[0,1]],  #   Y,
            [vertex[0,2]],  #   Z,
            [1.0]           #   1.0]]
        ])
        # Transform the points to camera.
        transformed_3d_pos = np.dot(inv(extrinsic_mat.matrix), pos_vector)
        # Transform the points to 2D.
        pos2d = np.dot(rgb_intrinsic, transformed_3d_pos[:3])

        # Normalize the 2D points.
        pos2d = np.array([
            pos2d[0] / pos2d[2],
            pos2d[1] / pos2d[2],
            pos2d[2]
        ])

        # Add the points to the image.
        if pos2d[2] > 0: # If the point is in front of the camera.
            x_2d = float(image_width - pos2d[0])
            y_2d = float(image_height - pos2d[1])
            if (x_2d >= 0 or y_2d >= 0) and (x_2d < image_width or y_2d < image_height):
                coords.append((x_2d, y_2d, pos2d[2]))

    return coords


def point_cloud_from_rgbd(depth_frame, depth_transform, world_transform):
    far = 1.0
    point_cloud = depth_to_local_point_cloud(
        depth_frame, color=None, max_depth=far)
    car_to_world_transform = world_transform * depth_transform
    point_cloud.apply_transform(car_to_world_transform)
    # filename = './point_cloud_tmp.ply'
    # point_cloud.save_to_disk(filename)
    # pcd = read_point_cloud(filename)
    # draw_geometries([pcd])
    return point_cloud


def get_3d_world_position(x, y, (image_width, image_height), depth_frame, depth_transform, world_transform):
    pc = point_cloud_from_rgbd(depth_frame,
                               depth_transform,
                               world_transform)
    return pc.array.tolist()[y * image_width + x]


def load_coco_labels(labels_path):
    labels_map = {}
    with open(labels_path) as labels_file:
        labels = labels_file.read().splitlines()
        index = 1
        for label in labels:
            labels_map[index] = label
            index += 1
    return labels_map


def get_pedestrian_bounding_box(corners):
    """
    Gets the bounding box of the pedestrian given the corners of the plane.
    """
    # Figure out the opposite ends of the rectangle. Our 2D mapping doesn't
    # return perfect rectangular coordinates and also doesn't return them
    # in clockwise order.
    max_distance = 0
    opp_ends = None
    for (a, b) in combinations(corners, r=2):
        if abs(a[0] - b[0]) <= 0.8 or abs(a[1] - b[1]) <= 0.8:
            # The points are too close. They may be lying on the same axis.
            # Move forward.
            pass
        else:
            # The points possibly lie on different axis. Choose the two
            # points which are the farthest.
            distance = (b[0] - a[0])**2 + (b[1] - a[1])**2
            if distance > max_distance:
                max_distance = distance
                if a[0] < b[0] and a[1] < b[1]:
                    opp_ends = (a, b)
                else:
                    opp_ends = (b, a)

    # If we were able to find two points far enough to be considered as
    # possible bounding boxes, return the results.
    return opp_ends


def get_bounding_box_sampling_points(ends):
    """
    Get the sampling points given the ends of the rectangle.
    """
    a, b = ends

    # Find the middle point of the rectangle, and see if the points
    # around it are visible from the camera.
    middle_point = ((a[0] + b[0]) / 2, (a[1] + b[1]) / 2,
                    b[2].flatten().item(0))
    sampling_points = [
        middle_point, 
        (middle_point[0] + 3, middle_point[1], middle_point[2]),
        (middle_point[0] + 1.5, middle_point[1] + 1.5, middle_point[2]),
        (middle_point[0] + 1.5, middle_point[1] - 1.5, middle_point[2]),
        (middle_point[0] - 3, middle_point[1], middle_point[2]),
        (middle_point[0] - 1.5, middle_point[1] + 1.5, middle_point[2]),
        (middle_point[0] - 1.5, middle_point[1] - 1.5, middle_point[2])
    ]
    return sampling_points


def have_same_depth(x, y, z, depth_array, threshold):
    x, y = int(x), int(y)
    return abs(depth_array[y][x] * 1000 - z) < threshold
