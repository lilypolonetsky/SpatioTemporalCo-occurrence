#!/usr/bin/env python3
from typing import Tuple

class LoadFile:
    # load the text file as an rdd
    def read_file(self, file_path: str):
        pass

    # map the text_file_rdd to be in the form (id, (x, y, z))
    def map_data(self, text_file_rdd):
        pass

class ProcessData:
    # direct all the data to all of its cubes (where it should be based on coords and duplicate across borders)
    # sort data in each cube
    # rendenzvous with each other -> (id_1, id_2, co_occur_instance) - use sliding window and hash coords of id_1 and id_2 to get unique instance
    pass

class OutputData:
    # given an rdd with (id_1, id_2, co_occur_instance), determine how to display the data to the user - histogram?
    pass
    
class Helpers:
    def __init__(self, space_size: int, time_size: int):
        pass

    # determine if item_1 and item_2 are from the same cube
    def same_cube(self, item_1, item_2) -> bool:
        pass

    # get the cube id for the given latitude, longitude, and time
    def get_cube(self, lat: float, longit: float, time: float) -> Tuple[int, int, int]:
        pass

    # get a unique id to represent the co-occurrence of item_1 and item_2 at the given (x,y,z)
    def get_uniq_instance_id(self, item_1: Tuple[int, int, int], item_2: Tuple[int, int, int]) -> int:
        pass

    # determine whether item should be duplicated to the right cube
    def duplicate_right(self, item: Tuple[int, int, int]) -> bool:
        pass

    # determine whether item should be duplicated to the bottom right cube
    def duplicate_bottom_right(self, item: Tuple[int, int, int]) -> bool:
        pass

    # determine whether item should be duplicated to the bottom cube
    def duplicate_bottom(self, item: Tuple[int, int, int]) -> bool:
        pass
