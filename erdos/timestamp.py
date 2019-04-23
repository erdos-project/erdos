class Timestamp(object):
    """A ERDOS timestamp.

       The timestamp can consist of one or more coordinates.

       Attributes:
           timestamp (Timestamp): For the copy constructor.
           coordinates (list of int): An array of coordinates.
    """

    def __init__(self, timestamp=None, coordinates=None):
        if timestamp is None:
            assert coordinates is not None, 'Timestamp has empty coordinates'
            self.coordinates = coordinates
        else:
            self.coordinates = timestamp.coordinates

    def __repr__(self):
        return str(self.coordinates)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, timestamp):
        if len(self.coordinates) != len(timestamp.coordinates):
            return False
        for coord, other_coord in zip(self.coordinates, timestamp.coordinates):
            if coord != other_coord:
                return False
        return True

    def __ne__(self, timestamp):
        return not self.__eq__(timestamp)

    def __lt__(self, timestamp):
        if len(self.coordinates) != len(timestamp.coordinates):
            raise Exception(
                'Cannot compare timestamps of different size {} and {}'.format(
                    self, timestamp))
        for coord, other_coord in zip(self.coordinates, timestamp.coordinates):
            if coord > other_coord:
                return False
            elif coord < other_coord:
                return True
        return False

    def __le__(self, timestamp):
        if len(self.coordinates) != len(timestamp.coordinates):
            raise Exception(
                'Cannot compare timestamps of different size {} and {}'.format(
                    self, timestamp))
        for coord, other_coord in zip(self.coordinates, timestamp.coordinates):
            if coord > other_coord:
                return False
            elif coord < other_coord:
                return True
        return True

    def __gt__(self, timestamp):
        return not self.__le__(timestamp)

    def __ge__(self, timestamp):
        return not self.__lt__(timestamp)

    def __hash__(self):
        return hash(tuple(self.coordinates))
