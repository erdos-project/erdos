import numpy as np

# Cityscapes palette.
CITYSCAPES_CLASSES = {
    0: [0, 0, 0],         # None
    1: [70, 70, 70],      # Buildings
    2: [190, 153, 153],   # Fences
    3: [72, 0, 90],       # Other
    4: [220, 20, 60],     # Pedestrians
    5: [153, 153, 153],   # Poles
    6: [157, 234, 50],    # RoadLines
    7: [128, 64, 128],    # Roads
    8: [244, 35, 232],    # Sidewalks
    9: [107, 142, 35],    # Vegetation
    10: [0, 0, 255],      # Vehicles
    11: [102, 102, 156],  # Walls
    12: [220, 220, 0]     # TrafficSigns
}


def transfrom_to_cityscapes(frame_array):
    result = np.zeros((frame_array.shape[0], frame_array.shape[1], 3))
    for key, value in CITYSCAPES_CLASSES.items():
        result[np.where(frame_array == key)] = value
    return result


def compute_semantic_iou(ground_frame, predicted_frame):
    iou = {}
    for key, value in CITYSCAPES_CLASSES.items():
        target = np.zeros((ground_frame.shape[0], ground_frame.shape[1], 3))
        prediction = np.zeros((ground_frame.shape[0], ground_frame.shape[1], 3))
        target[np.where(ground_frame == value)] = 1
        prediction[np.where(predicted_frame == value)] = 1
        intersection = np.logical_and(target, prediction)
        union = np.logical_or(target, prediction)
        sum_intersection = np.sum(intersection)
        sum_union = np.sum(union)
        # Ignore non-existing classes.
        if sum_union > 0:
            iou[key] = float(sum_intersection) / float(sum_union)
    mean_iou = np.mean(iou.values())
    return (mean_iou, iou)


def compute_instance_iou(ground_frame, predicted_frame):
    # iIou = iTP/(iTP + FP + iFN)
    # iTP and iFN are computed by weighting the contribution of each pixel
    # by the ratio of the class' average instance size to the size of the
    # respective ground truth instance.

    # TODO(ionel): Implement!
    pass
