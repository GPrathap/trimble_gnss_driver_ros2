#!/usr/bin/env python
"""
This script parses and converts Trimble GSOF messages incoming from a receiver and publishes the relevant ROS messages.
It has been adapted from https://kb.unavco.org/kb/article/trimble-netr9-receiver-gsof-messages-806.html

Authors:
- Michael Hutchinson (mhutchinson@sagarobotics.com)
- Geesara (ggeesara@gmail.com)
"""

import socket
import sys
import math
from struct import unpack

import rclpy
from rclpy.node import Node
from rclpy.duration import Duration
from rclpy.time import Time

from geometry_msgs.msg import Quaternion
from sensor_msgs.msg import NavSatFix, NavSatStatus, Imu

from trimble_gnss_driver_ros2.scripts.parser import parse_maps
from trimble_gnss_driver_ros2.scripts.gps_qualities import gps_qualities

# Constants
ATTITUDE = 27
LAT_LON_H = 2
POSITION_SIGMA = 12
POSITION_TYPE = 38
BASE_POSITION_QUALITY = 41
INS_FULL_NAV = 49
INS_RMS = 50
RECEIVED_BASE_INFO = 35

VELOCITY = 8
SERIAL_NUM = 15
GPS_TIME = 1
UTC_TIME = 16
ECEF_POS = 3
LOCAL_DATUM = 4
LOCAL_ENU = 5

class GSOFDriver(Node):
    """A class to parse GSOF messages from a TCP stream."""

    def __init__(self):
        super().__init__('gsof_driver')

        self.declare_parameters(
            namespace='',
            parameters=[
                ('rtk_port', 21098),
                ('rtk_ip', '192.168.0.50'),
                ('output_frame_id', 'base_link'),
                ('apply_dual_antenna_offset', False),
                ('gps_main_frame_id', 'back_antenna_link'),
                ('gps_aux_frame_id', 'front_antenna_link'),
                ('heading_offset', 0.0),
                ('prefix', 'gps')
            ]
        )

        self.rtk_port = self.get_parameter('rtk_port').value
        self.rtk_ip = self.get_parameter('rtk_ip').value
        self.output_frame_id = self.get_parameter('output_frame_id').value
        self.apply_dual_antenna_offset = self.get_parameter('apply_dual_antenna_offset').value
        self.heading_offset = self.get_parameter('heading_offset').value
        self.prefix = self.get_parameter('prefix').value
        self.gps_main_frame_id = self.get_parameter('gps_main_frame_id').value
        self.gps_aux_frame_id = self.get_parameter('gps_aux_frame_id').value

        if not self.apply_dual_antenna_offset:
            self.heading_offset = 0

        self.get_logger().info(f"Heading offset is {self.heading_offset}")

        self.fix_pub = self.create_publisher(NavSatFix, "/fix", 1)
        self.attitude_pub = self.create_publisher(Imu, "/attitude", 1)
        self.yaw_pub = self.create_publisher(Imu, '/yaw', 1)

        self.client = self.setup_connection(self.rtk_ip, self.rtk_port)

        self.msg_dict = {}
        self.msg_bytes = None
        self.checksum = True
        self.rec_dict = {}

        self.current_time = self.get_clock().now()
        self.ins_rms_ts = Time()
        self.pos_sigma_ts = Time()
        self.quality_ts = Time()
        self.error_info_timeout = Duration(seconds=1.0)
        self.base_info_timeout = Duration(seconds=5.0)

        while rclpy.ok():
            self.records = []
            self.current_time = self.get_clock().now()
            self.get_message_header()
            self.get_records()

            if not self.checksum:
                self.get_logger().warn("Invalid checksum, skipping")
                continue

            if INS_FULL_NAV in self.records:
                if INS_RMS in self.records or (self.current_time - self.ins_rms_ts) < self.error_info_timeout:
                    self.send_ins_fix()
                    self.send_ins_attitude()
                else:
                    self.get_logger().warn(
                        f"Skipping INS output as no matching errors within the timeout. Current time: {self.current_time}, last error msg {self.ins_rms_ts}")
            else:
                if LAT_LON_H in self.records:
                    if (POSITION_SIGMA in self.records or (self.current_time - self.pos_sigma_ts) < self.error_info_timeout) and (POSITION_TYPE in self.records or (self.current_time - self.quality_ts) < self.base_info_timeout):
                        self.send_fix()
                    else:
                        self.get_logger().warn(
                            f"Skipping fix output as no corresponding sigma errors or gps quality within the timeout. Current time: {self.current_time}, last sigma msg {self.pos_sigma_ts}, last gps quality msg {self.quality_ts}")
                if ATTITUDE in self.records:
                    self.send_yaw()

    def quaternion_from_euler(self, roll, pitch, yaw):
        cy = math.cos(yaw * 0.5)
        sy = math.sin(yaw * 0.5)
        cp = math.cos(pitch * 0.5)
        sp = math.sin(pitch * 0.5)
        cr = math.cos(roll * 0.5)
        sr = math.sin(roll * 0.5)

        q = [0] * 4
        q[0] = cy * cp * cr + sy * sp * sr
        q[1] = cy * cp * sr - sy * sp * cr
        q[2] = sy * cp * sr + cy * sp * cr
        q[3] = sy * cp * cr - cy * sp * sr

        return q

    def send_ins_fix(self):
        if self.rec_dict['FUSED_LATITUDE'] == 0 and self.rec_dict['FUSED_LONGITUDE'] == 0 and self.rec_dict['FUSED_ALTITUDE'] == 0:
            self.get_logger().warn("Invalid fix, skipping")
            return

        current_time = self.get_clock().now()
        fix = NavSatFix()

        fix.header.stamp = current_time.to_msg()
        fix.header.frame_id = self.output_frame_id

        gps_qual = gps_qualities[self.rec_dict['GPS_QUALITY']]
        fix.status.service = NavSatStatus.SERVICE_GPS
        fix.status.status = gps_qual[0]
        fix.position_covariance_type = gps_qual[1]

        fix.latitude = self.rec_dict['FUSED_LATITUDE']
        fix.longitude = self.rec_dict['FUSED_LONGITUDE']
        fix.altitude = self.rec_dict['FUSED_ALTITUDE']

        fix.position_covariance[0] = self.rec_dict['FUSED_RMS_LONGITUDE'] ** 2
        fix.position_covariance[4] = self.rec_dict['FUSED_RMS_LATITUDE'] ** 2
        fix.position_covariance[8] = self.rec_dict['FUSED_RMS_ALTITUDE'] ** 2

        self.fix_pub.publish(fix)

    def send_ins_attitude(self):
        if self.rec_dict['FUSED_ROLL'] == 0 and self.rec_dict['FUSED_PITCH'] == 0 and self.rec_dict['FUSED_YAW'] == 0:
            self.get_logger().warn("Invalid yaw, skipping")
            return

        current_time = self.get_clock().now()
        attitude = Imu()

        attitude.header.stamp = current_time.to_msg()
        attitude.header.frame_id = self.output_frame_id

        heading_enu = 2 * math.pi - self.normalize_angle(
            math.radians(self.rec_dict['FUSED_YAW']) + 3 * math.pi / 2)
        orie_quat = self.quaternion_from_euler(
            math.radians(self.rec_dict['FUSED_ROLL']),
            -math.radians(self.rec_dict['FUSED_PITCH']),
            heading_enu
        )

        attitude.orientation = Quaternion(
            x=orie_quat[1], y=orie_quat[2], z=orie_quat[3], w=orie_quat[0])

        attitude.orientation_covariance[0] = math.radians(
            self.rec_dict['FUSED_RMS_ROLL']) ** 2
        attitude.orientation_covariance[4] = math.radians(
            self.rec_dict['FUSED_RMS_PITCH']) ** 2
        attitude.orientation_covariance[8] = math.radians(
            self.rec_dict['FUSED_RMS_YAW']) ** 2

        self.attitude_pub.publish(attitude)

    def send_fix(self):
        if self.rec_dict['LATITUDE'] == 0 and self.rec_dict['LONGITUDE'] == 0 and self.rec_dict['HEIGHT_WGS84'] == 0:
            self.get_logger().warn("Invalid fix, skipping")
            return

        current_time = self.get_clock().now()
        fix = NavSatFix()

        fix.header.stamp = current_time.to_msg()
        fix.header.frame_id = self.output_frame_id
        gps_qual = self.get_gps_quality()
        fix.status.service = NavSatStatus.SERVICE_GPS
        fix.status.status = gps_qual[0]
        fix.position_covariance_type = gps_qual[1]

        fix.latitude = math.degrees(self.rec_dict['LATITUDE'])
        fix.longitude = math.degrees(self.rec_dict['LONGITUDE'])
        fix.altitude = self.rec_dict['HEIGHT_WGS84']

        fix.position_covariance[0] = self.rec_dict['SIGMA_LATITUDE'] ** 2
        fix.position_covariance[4] = self.rec_dict['SIGMA_LONGITUDE'] ** 2
        fix.position_covariance[8] = self.rec_dict['SIGMA_HEIGHT'] ** 2

        self.fix_pub.publish(fix)

    def get_gps_quality(self):
        return gps_qualities[self.rec_dict['QUALITY']]

    def send_yaw(self):
        if self.rec_dict['HEADING'] == 0:
            self.get_logger().warn("Invalid yaw, skipping")
            return

        current_time = self.get_clock().now()
        yaw = Imu()

        yaw.header.stamp = current_time.to_msg()
        yaw.header.frame_id = self.output_frame_id

        if self.apply_dual_antenna_offset:
            yaw_angle = self.normalize_angle(
                math.radians(self.rec_dict['HEADING']) + self.heading_offset)
        else:
            yaw_angle = self.normalize_angle(
                math.radians(self.rec_dict['HEADING']))

        orie_quat = self.quaternion_from_euler(0, 0, yaw_angle)
        yaw.orientation = Quaternion(
            x=orie_quat[1], y=orie_quat[2], z=orie_quat[3], w=orie_quat[0])

        self.yaw_pub.publish(yaw)

    def setup_connection(self, rtk_ip, rtk_port):
        self.get_logger().info(f"Connecting to {rtk_ip} on port {rtk_port}")
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            client.connect((rtk_ip, rtk_port))
        except socket.error as e:
            self.get_logger().error(f"Error connecting to socket: {e}")
            sys.exit(1)

        return client

    def get_message_header(self):
        data = self.client.recv(1)
        self.get_logger().debug(f"Data received: {data.hex()}")

        if data != b'\x02':
            self.get_logger().warn(f"Invalid start of header: {data.hex()}")
            return

        msg_dict = {
            "record_id": None,
            "length": None,
            "length_2": None
        }

        msg_dict["record_id"] = unpack('<B', self.client.recv(1))[0]
        msg_dict["length"] = unpack('<H', self.client.recv(2))[0]
        msg_dict["length_2"] = unpack('<H', self.client.recv(2))[0]

        self.msg_dict = msg_dict

    def get_records(self):
        self.records = []
        length = self.msg_dict["length"]

        self.msg_bytes = self.client.recv(length)
        checksum = self.msg_bytes[-3]
        self.msg_bytes = self.msg_bytes[:-3]

        checksum_calc = self.compute_checksum(self.msg_bytes)

        if checksum == checksum_calc:
            self.checksum = True
            parse_maps[self.msg_dict["record_id"]](self.msg_bytes, self.rec_dict)
            self.records.append(self.msg_dict["record_id"])
        else:
            self.checksum = False
            self.get_logger().warn("Checksum mismatch")

    @staticmethod
    def compute_checksum(data):
        checksum = 0
        for byte in data:
            checksum += byte
        checksum = (~checksum & 0xFF) + 1
        return checksum

    @staticmethod
    def normalize_angle(angle):
        while angle < -math.pi:
            angle += 2 * math.pi
        while angle > math.pi:
            angle -= 2 * math.pi
        return angle


def main(args=None):
    rclpy.init(args=args)
    node = GSOFDriver()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass

    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
