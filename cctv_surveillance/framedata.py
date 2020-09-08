import numpy
from typing import List
import datetime

# Each frameData describes a "face"
class FrameData:
    def __init__(self):
        self.raw_frame: bytes = b""
        self.t_created : datetime.datetime = datetime.datetime.now() 
        self.t_updated : datetime.datetime = self.t_created
        
        self.faces: List = []
        self.matched_faces: List[str]  = []  # people whom it matches to

    def update_timestamp(self):
       self.t_updated = datetime.datetime.now()



