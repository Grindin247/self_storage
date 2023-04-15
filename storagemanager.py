
#Linux OS Only
#Required system utils: lsblk, wipefs, parted, zpool

#TODO: Add rest interface
#TODO: Add backup worker
import json
import subprocess
import csv
import os
import logging
from logging.handlers import RotatingFileHandler
import time
import threading
import sys

#Configure Logger
logger = logging.getLogger("Self Storage Log")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
handler = RotatingFileHandler("self-storage.log", maxBytes=100*1024*1024, backupCount=2)
handler.setFormatter(formatter)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.addHandler(handler)

POOL_NAME = "self-storage"
DEVICE_RECORD_FILEPATH = ".storagedevices"

DEVICE_DETECT_POLL_INTERVAL_SEC = 10

DEV_STATUS_UNKNOWN     = "unknown"
DEV_STATUS_ONLINE      = "online"
DEV_STATUS_OFFLINE     = "offline"
DEV_STATUS_DEGRADED    = "degraded"
DEV_STATUS_UNAVAIL     = "unavailable"
DEV_STATUS_FAULTED     = "faulted"

LSBLK_FIELD_SERIAL  = "serial"
LSBLK_FIELD_NAME    = "name"
LSBLK_FIELD_VENDOR  = "vendor"
LSBLK_FIELD_MODEL   = "model"
LSBLK_FIELD_SIZE    = "size"

class BackupInfo:
    def __init__(self):
        self.deviceSerial = ""
        self.lastDatetime = ""
        self.errorStr = ""

class StorageDevice:    
    def __init__(self, serial, name, vendor, model, size):
        self.serial = serial
        self.name = name
        self.vendor = vendor
        self.model = model
        self.size = size
        self.reliable = True
        self.inpool = False
        self.connected = False
        self.backupInfo = BackupInfo()
        self.status = DEV_STATUS_UNKNOWN

managedDevices = {}

def deviceManagementWorker():
    global managedDevices

    while True:
        logger.debug("Managing devices ...")

        lsblkOut = subprocess.check_output(["lsblk", "-JO"])
        blkDevices = json.loads(lsblkOut)["blockdevices"]

        if len(managedDevices) > 0:
            logger.debug("storage pool detected")
        #Handle removed devices - All devices in the pool must be attached 
            tempDevList =  [dev.serial for dev in managedDevices.values() if dev.inpool]
            logger.debug("devices in pool: {}".format(" ".join(tempDevList)))
            for device in blkDevices:
                if device[LSBLK_FIELD_SERIAL] in tempDevList:
                    tempDevList.remove(device[LSBLK_FIELD_SERIAL])

            if len(tempDevList) > 0:
                logger.debug("One or more devices removed")
                for removedDevice in tempDevList:
                    managedDevices[removedDevice].connected = False
                    logger.error("Device: {}/{} DISCONNECTED!".format(managedDevices[removedDevice].name, removedDevice))
                continue
        
        deviceAdded = False
        for device in blkDevices:
            if (device["tran"] == "usb" or device["rm"]) and not device["ro"]: #If usb device or removable media but not read-only
                if device[LSBLK_FIELD_SERIAL] not in managedDevices.keys():
                    logger.debug(" ".join([device[LSBLK_FIELD_SERIAL],
                            device[LSBLK_FIELD_NAME],
                            device[LSBLK_FIELD_VENDOR],
                            device[LSBLK_FIELD_MODEL],
                            device[LSBLK_FIELD_SIZE]]))

                    try:
                        #Wipe device
                        wipefsOut = subprocess.check_output(["wipefs", "--all", "/dev/"+device["name"]])
                        logger.debug(wipefsOut)

                        #Configure device
                        partedOut = subprocess.check_output(["parted", "-a", "optimal", "/dev/"+device["name"], "mklabel", "msdos", "mkpart", "primary", "0%", "100%"])
                        logger.debug(partedOut)

                        #Add to pool
                        zpoolOut = subprocess.check_output(["zpool", "create" if  len(managedDevices) == 0 else "add", POOL_NAME, device["name"]])
                        logger.debug(zpoolOut)

                        #TODO: Get device status

                        managedDevices[device[LSBLK_FIELD_SERIAL]] = StorageDevice(device[LSBLK_FIELD_SERIAL].strip(),
                                                                    device[LSBLK_FIELD_NAME].strip(),
                                                                    device[LSBLK_FIELD_VENDOR].strip(),
                                                                    device[LSBLK_FIELD_MODEL].strip(),
                                                                    device[LSBLK_FIELD_SIZE])
                        managedDevices[device[LSBLK_FIELD_SERIAL]].inpool = True
                        managedDevices[device[LSBLK_FIELD_SERIAL]].connected = True

                        with open(DEVICE_RECORD_FILEPATH, "w") as f:
                            csvwriter = csv.writer(f)
                            for dev in managedDevices.values():
                                csvwriter.writerow([dev.serial, 
                                                    dev.name,
                                                    dev.vendor,
                                                    dev.model, 
                                                    dev.size,
                                                    dev.reliable,
                                                    dev.inpool,
                                                    dev.connected,
                                                    dev.backupInfo.deviceSerial,
                                                    dev.backupInfo.lastDatetime,
                                                    dev.backupInfo.errorStr,
                                                    dev.status])
                        deviceAdded = True
                    except subprocess.CalledProcessError as e:
                        logger.error(e)
        if deviceAdded:
             smbOut = subprocess.check_output(["systemctl", "restart", "smbd.service"])
             logger.debug(smbOut)
             
        time.sleep(DEVICE_DETECT_POLL_INTERVAL_SEC)

if os.path.exists(DEVICE_RECORD_FILEPATH):
    with open(DEVICE_RECORD_FILEPATH, "r") as f:
        csvreader = csv.reader(f)
        for row in csvreader:
            managedDevices[row[0]] = StorageDevice(row[0],
                                                        row[1],
                                                        row[2],
                                                        row[3],
                                                        row[4])
            managedDevices[row[0]].reliable = row[5]
            managedDevices[row[0]].inpool = row[6]
            managedDevices[row[0]].connected = row[7]
            managedDevices[row[0]].backupInfo.deviceSerial = row[8]
            managedDevices[row[0]].backupInfo.deviceSerial = row[9]
            managedDevices[row[0]].backupInfo.deviceSerial = row[10]
            managedDevices[row[0]].status = row[11]

devManThread = threading.Thread(target=deviceManagementWorker, name="Device Management")
devManThread.setDaemon(True) 
devManThread.start() # Thread name

#TODO: Add REST Server 
while True:
    time.sleep(10)