#!/usr/bin/env python3
import pyspark
from typing import Tuple, List
import time
from collections import deque
from calendar import timegm
import sys
from decimal import Decimal
from math import ceil, floor

Lat = int
Longit = int
Time = int
CubeData = Tuple[Lat, Longit, Time]
CubeId = str
UserId = str
UserData = Tuple[UserId,Lat,Longit,Time]

# determine if userA and userB spatially co-occur
def spatialCoocur(userA: UserData, userB: UserData) -> bool:
    xDif = abs(userA[1] - userB[1])
    yDif = abs(userA[2] - userB[2])

    return xDif <= spaceCrit and yDif <= spaceCrit

# convert timestamp to epoch time
def getEpochTime(timestamp):
    pattern = "%Y-%m-%dT%H:%M:%SZ"
    utcTime = time.strptime(timestamp, pattern)
    return timegm(utcTime)

# determine what cube the coordinates belong to by calculating the top left corner's coordinates
def getCube(lat, longit, time):
    spaceWindow = Decimal(spaceCrit * 2)
    timeWindow = Decimal(timeCrit * 2)
    
    return ((floor(lat / spaceWindow), floor(longit / spaceWindow), floor(time / timeWindow)))

# determine if the coordinates should be duplicated to the right by checking if it is on the righthand side of the cube
def duplicateRight(lat, longit, time):
    curCubeX, curCubeY, curCubeZ = getCube(lat, longit, time)
    halfCurCubeY = 2 * spaceCrit * curCubeY + spaceCrit

    return longit >= halfCurCubeY

# determine if the coordinates should be duplicated to the bottom by checking if it is on the bottom of the cube
def duplicateBottom(lat, longit, time):
    curCubeX, curCubeY, curCubeZ = getCube(lat, longit, time)   # represents the bottom left corner of the cube
    halfCurCubeZ = 2 * timeCrit * curCubeZ + timeCrit           # calculate the coords of the current cube + timeCrit

    return time <= halfCurCubeZ                                 # is time in the bottom half of the cube?

# determine if the coordinates should be duplicated forward by checking if it is on the front side of the cube
def duplicateFront(lat, longit, time):
    curCubeX, curCubeY, curCubeZ = getCube(lat, longit, time)
    halfCurCubeX = 2 * spaceCrit * curCubeX + spaceCrit

    return lat >= halfCurCubeX

# determine if the coordinates should be duplicated to the cube that is in front and to the right of this cube by checking if it is on the front and right side of the cube
def duplicateFrontRight(lat, longit, time):
    curCubeX, curCubeY, curCubeZ = getCube(lat, longit, time)
    halfCurCubeX = 2 * spaceCrit * curCubeX + spaceCrit
    halfCurCubeY = 2 * spaceCrit * curCubeY + spaceCrit

    return lat >= halfCurCubeX and longit >= halfCurCubeY

# determine if the coordinates should be duplicated to the cube that is in front and below this cube by checking if it is on the front and bottom side of the cube
def duplicateFrontBottom(lat, longit, time):
    curCubeX, curCubeY, curCubeZ = getCube(lat, longit, time)
    halfCurCubeX = 2 * spaceCrit * curCubeX + spaceCrit
    halfCurCubeZ = 2 * timeCrit * curCubeZ + timeCrit

    return time <= halfCurCubeZ and lat >= halfCurCubeX

# determine if the coordinates should be duplicated to the cube that is in below and to the right of this cube by checking if it is on the bottom and right side of the cube
def duplicateBottomRight(lat, longit, time):
    curCubeX, curCubeY, curCubeZ = getCube(lat, longit, time)
    halfCurCubeY = 2 * spaceCrit * curCubeY + spaceCrit
    halfCurCubeZ = 2 * timeCrit * curCubeZ + timeCrit

    return time <= halfCurCubeZ and longit >= halfCurCubeY

# determine if the coordinates should be duplicated to the cube that is in front, to the right, and below this cube by checking if it is on the front, right, and bottom side of the cube
def duplicateBottomRightCorner(lat, longit, time):
    curCubeX, curCubeY, curCubeZ = getCube(lat, longit, time)
    halfCurCubeX = 2 * spaceCrit * curCubeX + spaceCrit
    halfCurCubeY = 2 * spaceCrit * curCubeY + spaceCrit
    halfCurCubeZ = 2 * timeCrit * curCubeZ + timeCrit

    return lat >= halfCurCubeX and longit >= halfCurCubeY and time <= halfCurCubeZ

# move the coordinate over to the next cube
def shift(coord, z=False):
    if z:
      return (coord * timeCrit * 2 - timeCrit) // (timeCrit * 2)
    else:
        return ((coord * spaceCrit * 2) + (spaceCrit * 2)) // (spaceCrit * 2)

# get the coordinates for the cube that is to the right of the current coordinates
def getRightCube(x, y, z):
    return getCoordString(x, shift(y), z)

# get the coordinates for the cube that is below the current coordinates
def getBottomCube(x, y, z):
    return getCoordString(x, y, shift(z, True))

# get the coordinates for the cube that is in front of the current coordinates
def getFrontCube(x, y, z):
    return getCoordString(shift(x), y, z)

# get the coordinates for the cube that is in front and to the right of the current coordinates
def getFrontRightCube(x, y, z):
    return getCoordString(shift(x), shift(y), z)

# get the coordinates for the cube that is in front and below the current coordinates
def getFrontBottomCube(x, y, z):
    return getCoordString(shift(x), y, shift(z, True))

# get the coordinates for the cube that is below and to the right of the current coordinates
def getBottomRightCornerCube(x, y, z):
    return getCoordString(shift(x), shift(y), shift(z, True))

# get the coordinates for the cube that is to the right and below the current coordinates
def getBottomRightCube(x, y, z):
    return getCoordString(x, shift(y), shift(z, True))

def getCoordNums(cubeStr):
    return cubeStr.split(",")

def getCoordString(x, y=None, z=None):
    if not y and not z:
        return ",".join([str(val) for val in x])
    if y and z:
        return ",".join([str(x),str(y),str(z)])

# create a list of all the cubes that the coordinates should be duplicated to
def getAllCubes(lat, longit, time):    
    lat = Decimal(lat)
    longit = Decimal(longit)
    time = int(time)
    
    originCoords = getCube(lat, longit, time)
    cubes = [getCoordString(*originCoords)]

    if duplicateRight(lat, longit, time): cubes.append(getRightCube(*originCoords))
    if duplicateBottom(lat, longit, time): cubes.append(getBottomCube(*originCoords))
    if duplicateFrontRight(lat, longit, time): cubes.append(getFrontRightCube(*originCoords))
    if duplicateBottomRightCorner(lat, longit, time): cubes.append(getBottomRightCornerCube(*originCoords))
    if duplicateFront(lat, longit, time): cubes.append(getFrontCube(*originCoords))
    if duplicateBottomRight(lat, longit, time): cubes.append(getBottomRightCube(*originCoords))
    if duplicateFrontBottom(lat, longit, time): cubes.append(getFrontBottomCube(*originCoords))

    return cubes

def userInfo(data):
    fields = data.split()
    user = fields[0]
    time = getEpochTime(fields[1])
    lat = Decimal(fields[2])
    longit = Decimal(fields[3])

    return ((user, lat, longit, time), getAllCubes(lat, longit, time))

def findCoOccur(info: Tuple[CubeId, List[UserData]]):
    originCube,data = info
    window = deque([data[0]])
    curIndex = 1
    coOccurs = set()

    while curIndex < len(data):
        user = data[curIndex]
        
        while len(window) > 0 and (abs(user[3] - window[0][3])) > timeCrit:
            window.popleft()

        for otherUser in window:
            if otherUser[0] != user[0] and spatialCoocur(user, otherUser):
                # the smaller user should always be first to avoid separate counts of a pair                
                if otherUser[0] < user[0]:
                    coOccurs.add((otherUser[0], otherUser[3], user[0], user[3]))
                else:
                    coOccurs.add((user[0], user[3], otherUser[0], otherUser[3]))
            
        window.append(user)
        curIndex += 1

    return coOccurs

def to_list(a):
    return sorted([a])

def append(a, b):
    a.append(b)
    return sorted(a)

def extend(a, b):
    a.extend(b)
    return sorted(a)

sc = pyspark.SparkContext()
spaceCrit = Decimal(sys.argv[1])
timeCrit = Decimal(sys.argv[2])
data = sc.textFile(sys.argv[3])

# ((user, lat, longit, time), ["cube1x,cube1y,cube1z","cube2x,cube2y,cube2z"]) - map the user to all the cubes it should be duplicated to
mappedData = data.map(userInfo)
# ('cubeX,cubeY,cubeZ', (user, lat, longit, time)) - create a separate tuple for each cube that a user needs to go to
cubeMapping = mappedData.flatMapValues(lambda value: value).map(lambda x: (x[1],x[0]))
# ('cubeX,cubeY,cubeZ', [(user1, lat1, longit1, time1),(user2, lat2, longit2, time2)] - get all the data from the same cube together and sorted on time
cubeList = cubeMapping.combineByKey(to_list, append, extend).map(lambda x: (x[0], sorted(x[1], key= lambda y: y[3]))).filter(lambda x: len(x[1]) > 1)
# {('user1', time1, 'user2', time2),...} - create pairs of co-occurrences
coOccurList = cubeList.map(findCoOccur).filter(lambda x: len(x) != 0)
# (user1|user2, 1) - get rid of times, add a 1 for counting
coOccurInstances = coOccurList.flatMap(lambda value: value).distinct().map(lambda x: (x[0] + "|" + x[2], 1))
# (user1|user2, count) - sum all user pairs
instanceCount = coOccurInstances.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False)

print(instanceCount.collect())
print(instanceCount.count())
