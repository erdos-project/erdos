from itertools import combinations
import math
import numpy as np
from numpy.linalg import inv
from numpy.matlib import repmat

import carla.carla_server_pb2
from carla.sensor import Camera, PointCloud
from carla.transform import Transform


# We are transforming protobuf objects to python objects and
# back because we cannot pickle protobuf objects.
class Transform(object):
    def __init__(self, transform):
        self.location = (transform.location.x, transform.location.y,
                         transform.location.z)
        self.orientation = (transform.orientation.x,
                            transform.orientation.y,
                            transform.orientation.z)
        self.rotation = (transform.rotation.pitch,
                         transform.rotation.yaw,
                         transform.rotation.roll)

    def populate_pb2(self, transform):
        transform.location.x = self.location[0]
        transform.location.y = self.location[1]
        transform.location.z = self.location[2]
        transform.orientation.x = self.orientation[0]
        transform.orientation.y = self.orientation[1]
        transform.orientation.z = self.orientation[2]
        transform.rotation.pitch = self.rotation[0]
        transform.rotation.yaw = self.rotation[1]
        transform.rotation.roll = self.rotation[2]

    def to_transform_pb2(self):
        transform = carla.carla_server_pb2.Transform()
        self.populate_pb2(transform)
        return transform

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "location: {}, orientation: {}, rotation: {}".format(self.location, self.orientation, self.rotation)

class BoundingBox(object):
    def __init__(self, bb):
        self.transform = Transform(bb.transform)
        self.extent = (bb.extent.x, bb.extent.y, bb.extent.z)

    def to_bounding_box_pb2(self):
        bb = carla.carla_server_pb2.BoundingBox()
        self.transform.populate_pb2(bb.transform)
        bb.extent.x = self.extent[0]
        bb.extent.y = self.extent[1]
        bb.extent.z = self.extent[2]
        return bb

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "transform: {}, x: {}, y: {}, z: {}".format(str(self.transform), *self.extent)


def depth_to_local_point_cloud(depth_msg, color=None, max_depth=0.9):
    far = 1000.0  # max depth in meters.
    normalized_depth = depth_msg.frame
    # (Intrinsic) K Matrix
    k = np.identity(3)
    k[0, 2] = depth_msg.width / 2.0
    k[1, 2] = depth_msg.height / 2.0
    k[0, 0] = k[1, 1] = depth_msg.width / \
        (2.0 * math.tan(depth_msg.fov * math.pi / 360.0))
    # 2d pixel coordinates
    pixel_length = depth_msg.width * depth_msg.height
    u_coord = repmat(np.r_[depth_msg.width-1:-1:-1],
                     depth_msg.height, 1).reshape(pixel_length)
    v_coord = repmat(np.c_[depth_msg.height-1:-1:-1],
                     1, depth_msg.width).reshape(pixel_length)
    if color is not None:
        color = color.reshape(pixel_length, 3)
    normalized_depth = np.reshape(normalized_depth, pixel_length)

    # Search for pixels where the depth is greater than max_depth to
    # delete them
    max_depth_indexes = np.where(normalized_depth > max_depth)
    normalized_depth = np.delete(normalized_depth, max_depth_indexes)
    u_coord = np.delete(u_coord, max_depth_indexes)
    v_coord = np.delete(v_coord, max_depth_indexes)
    if color is not None:
        color = np.delete(color, max_depth_indexes, axis=0)

    # pd2 = [u,v,1]
    p2d = np.array([u_coord, v_coord, np.ones_like(u_coord)])

    # P = [X,Y,Z]
    p3d = np.dot(np.linalg.inv(k), p2d)
    p3d *= normalized_depth * far

    # Formating the output to:
    # [[X1,Y1,Z1,R1,G1,B1],[X2,Y2,Z2,R2,G2,B2], ... [Xn,Yn,Zn,Rn,Gn,Bn]]
    if color is not None:
        # np.concatenate((np.transpose(p3d), color), axis=1)
        return PointCloud(
            depth_msg.timestamp.coordinates[0],
            np.transpose(p3d),
            color_array=color)
    # [[X1,Y1,Z1],[X2,Y2,Z2], ... [Xn,Yn,Zn]]
    return PointCloud(depth_msg.timestamp.coordinates[0], np.transpose(p3d))


def point_cloud_from_rgbd(depth_msg, world_transform):
    far = 1.0
    point_cloud = depth_to_local_point_cloud(
        depth_msg, color=None, max_depth=far)
    car_to_world_transform = world_transform * depth_msg.transform
    point_cloud.apply_transform(car_to_world_transform)
    # filename = './point_cloud_tmp.ply'
    # point_cloud.save_to_disk(filename)
    # pcd = read_point_cloud(filename)
    # draw_geometries([pcd])
    return point_cloud


def get_3d_world_position(x, y, depth_msg, world_transform):
    pc = point_cloud_from_rgbd(depth_msg, world_transform)
    return pc.array.tolist()[y * depth_msg.width + x]


def get_camera_intrinsic_and_transform(name,
                                       postprocessing,
                                       field_of_view=90.0,
                                       image_size=(800, 600),
                                       position=(2.0, 0.0, 1.4),
                                       rotation_pitch=0,
                                       rotation_roll=0,
                                       rotation_yaw=0):
    camera = Camera(
        name,
        PostProcessing=postprocessing,
        FOV=field_of_view,
        ImageSizeX=image_size[0],
        ImageSizeY=image_size[1],
        PositionX=position[0],
        PositionY=position[1],
        PositionZ=position[2],
        RotationPitch=rotation_pitch,
        RotationRoll=rotation_roll,
        RotationYaw=rotation_yaw)

    image_width = image_size[0]
    image_height = image_size[1]
    # (Intrinsic) K Matrix
    intrinsic_mat = np.identity(3)
    intrinsic_mat[0][2] = image_width / 2
    intrinsic_mat[1][2] = image_height / 2
    intrinsic_mat[0][0] = intrinsic_mat[1][1] = image_width / (2.0 * math.tan(90.0 * math.pi / 360.0))
    return (intrinsic_mat, camera.get_unreal_transform(), (image_width, image_height))


def get_bounding_box_from_corners(corners):
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
        (middle_point[0] + 2, middle_point[1], middle_point[2]),
        (middle_point[0] + 1, middle_point[1] + 1, middle_point[2]),
        (middle_point[0] + 1, middle_point[1] - 1, middle_point[2]),
        (middle_point[0] - 2, middle_point[1], middle_point[2]),
        (middle_point[0] - 1, middle_point[1] + 1, middle_point[2]),
        (middle_point[0] - 1, middle_point[1] - 1, middle_point[2])
    ]
    return (middle_point, sampling_points)


def get_2d_bbox_from_3d_box(
        depth_array, world_transform, obj_transform,
        bounding_box, rgb_transform, rgb_intrinsic, rgb_img_size,
        middle_depth_threshold, neighbor_threshold):
    corners = map_ground_bounding_box_to_2D(
        depth_array, world_transform, obj_transform,
        bounding_box, rgb_transform, rgb_intrinsic,
        rgb_img_size)
    if len(corners) == 8:
        ends = get_bounding_box_from_corners(corners)
        if ends:
            (middle_point, points) = get_bounding_box_sampling_points(ends)
            # Select bounding box if the middle point in inside the frame
            # and has the same depth
            if (inside_image(middle_point[0], middle_point[1],
                             rgb_img_size[0], rgb_img_size[1]) and
                have_same_depth(middle_point[0],
                                middle_point[1],
                                middle_point[2],
                                depth_array,
                                middle_depth_threshold)):
                (xmin, xmax, ymin, ymax) = select_max_bbox(ends)
                width = xmax - xmin
                height = ymax - ymin
                # Filter out the small bounding boxes (they're far away).
                # We use thresholds that are proportional to the image size.
                # XXX(ionel): Reduce thresholds to 0.01, 0.01, and 0.0002 if
                # you want to include objects that are far away.
                if (width > rgb_img_size[0] * 0.01 and
                    height > rgb_img_size[1] * 0.02 and
                    width * height > rgb_img_size[0] * rgb_img_size[1] * 0.0004):
                    return (xmin, xmax, ymin, ymax)
            else:
                # The mid point doesn't have the same depth. It can happen
                # for valid boxes when the mid point is between the legs.
                # In this case, we check that a fraction of the neighbouring
                # points have the same depth.
                # Filter the points inside the image.
                points_inside_image = [
                    (x, y, z)
                    for (x, y, z) in points if inside_image(
                            x, y, rgb_img_size[0], rgb_img_size[1])
                ]
                same_depth_points = [
                    have_same_depth(x, y, z, depth_array, neighbor_threshold)
                            for (x, y, z) in points_inside_image
                ]
                if len(same_depth_points) > 0 and \
                        same_depth_points.count(True) >= 0.4 * len(same_depth_points):
                    (xmin, xmax, ymin, ymax) = select_max_bbox(ends)
                    width = xmax - xmin
                    height = ymax - ymin
                    width = xmax - xmin
                    height = ymax - ymin
                    # Filter out the small bounding boxes (they're far away).
                    # We use thresholds that are proportional to the image size.
                    # XXX(ionel): Reduce thresholds to 0.01, 0.01, and 0.0002 if
                    # you want to include objects that are far away.
                    if (width > rgb_img_size[0] * 0.01 and
                        height > rgb_img_size[1] * 0.02 and
                        width * height > rgb_img_size[0] * rgb_img_size[1] * 0.0004):
                        return (xmin, xmax, ymin, ymax)


def have_same_depth(x, y, z, depth_array, threshold):
    x, y = int(x), int(y)
    return abs(depth_array[y][x] * 1000 - z) < threshold


def inside_image(x, y, img_width, img_height):
    return x >= 0 and y >= 0 and x < img_width and y < img_height


def select_max_bbox(ends):
    (xmin, ymin) = tuple(map(int, ends[0][:2]))
    (xmax, ymax) = tuple(map(int, ends[0][:2]))
    corner = tuple(map(int, ends[1][:2]))
    # XXX(ionel): This is not quite correct. We get the
    # minimum and maximum x and y values, but these may
    # not be valid points. However, it works because the
    # bboxes are parallel to x and y axis.
    xmin = min(xmin, corner[0])
    ymin = min(ymin, corner[1])
    xmax = max(xmax, corner[0])
    ymax = max(ymax, corner[1])
    return (xmin, xmax, ymin, ymax)


def map_ground_bounding_box_to_2D(distance_img,
                                  world_transform,
                                  obstacle_transform,
                                  bounding_box,
                                  rgb_transform,
                                  rgb_intrinsic,
                                  rgb_img_size):
    (image_width, image_height) = rgb_img_size
    extrinsic_mat = world_transform * rgb_transform
    obj_transform = carla.transform.Transform(obstacle_transform.to_transform_pb2())
    bbox_pb2 = bounding_box.to_bounding_box_pb2()
    bbox_transform = carla.transform.Transform(bbox_pb2.transform)
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


def map_ground_3D_transform_to_2D(world_transform,
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
