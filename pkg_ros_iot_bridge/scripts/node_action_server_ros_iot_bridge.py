#!/usr/bin/env python

# ROS Node - Action Server - IoT ROS Bridge
import json
import threading
import rospy
import actionlib
import requests

# Message Class that is used by ROS Actions internally
from pkg_ros_iot_bridge.msg import msgRosIotAction
# Message Class that is used for Goal Messages
from pkg_ros_iot_bridge.msg import msgRosIotGoal
# Message Class that is used for Result Messages
from pkg_ros_iot_bridge.msg import msgRosIotResult
# Message Class that is used for Feedback Messages
from pkg_ros_iot_bridge.msg import msgRosIotFeedback

# Message Class for MQTT Subscription Messages
from pkg_ros_iot_bridge.msg import msgMqttSub

# Custom Python Module to perfrom MQTT Tasks
from pyiot import iot


class IotRosBridgeActionServer:
    """
    This class carries out all the functions of the combining the ros nodes and IOT protocols.
    It acts as a bridge between the ROS and MQTT and HTTP protocols

    Attributes:
        _as (object): It is the object of the ActionServer after calling its constructor

        _config_mqtt_server_url (str): It contains the MQTT server url

        _config_mqtt_server_port (str): It contains the MQTT server port

        _config_mqtt_sub_topic (str): It contains the topic used for subscribing

        _config_mqtt_pun_topic (str): It contains the topic used for publishing data

        _config_mqtt_sub_cb_ros_topic (str): It contains the ROS topic for
        communicating with the ROS nodes


    """

    # Constructor
    def __init__(self):
        """
        The constructor of class IotRosBridgeActionServer
        """
        # Initialize the Action Server
        self._as = actionlib.ActionServer('/action_ros_iot',
                                          msgRosIotAction,
                                          self.on_goal,
                                          self.on_cancel,
                                          auto_start=False)

        '''
            * self.on_goal - It is the fuction pointer which points to a function which will be
                             called when the Action Server receives a Goal

            * self.on_cancel - It is the fuction pointer which points to a function which will be
                             called when the Action Server receives a Cancel Request
        '''

        # Read and Store IoT Configuration data from Parameter Server
        param_config_iot = rospy.get_param('config_pyiot')
        self._config_mqtt_server_url = param_config_iot['mqtt']['server_url']
        self._config_mqtt_server_port = param_config_iot['mqtt']['server_port']
        self._config_mqtt_sub_topic = param_config_iot['mqtt']['topic_sub']
        self._config_mqtt_pub_topic = param_config_iot['mqtt']['topic_pub']
        self._config_mqtt_qos = param_config_iot['mqtt']['qos']
        self._config_mqtt_sub_cb_ros_topic = param_config_iot['mqtt']['sub_cb_ros_topic']
        print param_config_iot


        # Initialize ROS Topic Publication
        # Incoming message from MQTT Subscription will be published on a ROS Topic
        # (/ros_iot_bridge/mqtt/sub).
        # ROS Nodes can subscribe to this ROS Topic (/ros_iot_bridge/mqtt/sub)
        # to get messages from MQTT Subscription.
        self._handle_ros_pub = rospy.Publisher(self._config_mqtt_sub_cb_ros_topic,
                                               msgMqttSub, queue_size=10)


        # Subscribe to MQTT Topic (eyrc/xYzqLm/iot_to_ros)
        #  which is defined in 'config_iot_ros.yaml'.
        # self.mqtt_sub_callback() function will be called
        # when there is a message from MQTT Subscription.
        ret = iot.mqtt_subscribe_thread_start(self.mqtt_sub_callback,
                                              self._config_mqtt_server_url,
                                              self._config_mqtt_server_port,
                                              self._config_mqtt_sub_topic,
                                              self._config_mqtt_qos)
        if ret == 0:
            rospy.loginfo("MQTT Subscribe Thread Started")
        else:
            rospy.logerr("Failed to start MQTT Subscribe Thread")


        # Start the Action Server
        self._as.start()

        rospy.loginfo("Started ROS-IoT Bridge Action Server.")


    # This is a callback function for MQTT Subscriptions
    def mqtt_sub_callback(self, client, userdata, message):

        """
        This is the callback function whenever a new message arrives
        through the ROS topic

        Parameters:
            userdata (object): a user provided object that will be passed to the
                mqtt_sub_callback when a message is received.
            message (object): an object containing data of the message recieved over
                the mqtt topic
        """
        rospy.loginfo(userdata)
        rospy.loginfo(client)
        payload = str(message.payload.decode("utf-8"))

        print("[MQTT SUB CB] Message: ", payload)
        print("[MQTT SUB CB] Topic: ", message.topic)

        msg_mqtt_sub = msgMqttSub()
        msg_mqtt_sub.timestamp = rospy.Time.now()
        msg_mqtt_sub.topic = message.topic
        msg_mqtt_sub.message = payload

        self._handle_ros_pub.publish(msg_mqtt_sub)



    def on_goal(self, goal_handle):
        """
        This function will be called when Action Server receives a Goal
        from /action_ros_iot client
        This pushes the data incoming from the Action client into the respective sheets
        via the http protocol.
        It can also push data to MQTT topics if the protocol given is "mqtt"

        Parameters:
            goal_handle (object): It is the goal handle that corresponds to the goal
                that is sent by the action client
        """
        goal = goal_handle.get_goal()

        rospy.loginfo("Received new goal from Client in IOT BRIDGE")
        print('goaaaaaaaaaaaal', goal)

        #pushing data to google sheet

        if goal.protocol == 'http':
            goal_handle.set_accepted()
            try:
                parameters = json.loads(goal.message)
            except:
                rospy.logerr(goal)

            URL = "https://script.google.com/macros/s/AKfycbw5xylppoda-8HPjt2Tzq4ShU_Xef-Ik-hEtBPcPk0gdGw8095j4RZ7/exec"
            response = requests.get(URL, params=parameters)
            print response.content
            
            URL = "https://script.google.com/macros/s/AKfycbzdIlFmA4AtvM18f7WBMwdzzvkrS4HvBC2xb2LCMG8Jah6BE9IWAtod/exec"
            response = requests.get(URL, params=parameters)
            print response.content

            result = msgRosIotResult()
            result.flag_success = True
            goal_handle.set_succeeded(result)



        # Validate incoming goal parameters
        elif goal.protocol == "mqtt":

            if((goal.mode == "pub") or (goal.mode == "sub")):
                goal_handle.set_accepted()

                # Start a new thread to process new goal from the client
                #  (For Asynchronous Processing of Goals)
                # 'self.process_goal' - is the function pointer which points
                # to a function that will process incoming Goals
                thread = threading.Thread(name="worker",
                                          target=self.process_goal,
                                          args=(goal_handle,))
                thread.start()


            else:
                goal_handle.set_rejected()
                return

        else:
            goal_handle.set_rejected()
            return

    def process_goal(self, goal_handle):
        """
        This function process the goal from incoming from the Action client and then
        sends appropriate results to the action client based on whether the goal has been
        completed or failed.

        Parameters:
            goal_handle (object): This is the goal handle of the goal incoming from the
                Acion client
        """
        result = msgRosIotResult()

        goal_id = goal_handle.get_goal_id()
        rospy.loginfo("Processing goal : " + str(goal_id.id))

        goal = goal_handle.get_goal()


        # Goal Processing
        if goal.protocol == "mqtt":
            rospy.logwarn("MQTT")

            if goal.mode == "pub":
                rospy.logwarn("MQTT PUB Goal ID: " + str(goal_id.id))

                rospy.logwarn(goal.topic + " > " + goal.message)

                ret = iot.mqtt_publish(self._config_mqtt_server_url,
                                       self._config_mqtt_server_port,
                                       goal.topic,
                                       goal.message,
                                       self._config_mqtt_qos)

                if ret == 0:
                    rospy.loginfo("MQTT Publish Successful.")
                    result.flag_success = True
                else:
                    rospy.logerr("MQTT Failed to Publish")
                    result.flag_success = False

            elif goal.mode == "sub":
                rospy.logwarn("MQTT SUB Goal ID: " + str(goal_id.id))
                rospy.logwarn(goal.topic)

                ret = iot.mqtt_subscribe_thread_start(self.mqtt_sub_callback,
                                                      self._config_mqtt_server_url,
                                                      self._config_mqtt_server_port,
                                                      goal.topic,
                                                      self._config_mqtt_qos)
                if ret == 0:
                    rospy.loginfo("MQTT Subscribe Thread Started")
                    result.flag_success = True
                else:
                    rospy.logerr("Failed to start MQTT Subscribe Thread")
                    result.flag_success = False

        rospy.loginfo("Send goal result to client")
        if result.flag_success is True:
            rospy.loginfo("Succeeded")
            goal_handle.set_succeeded(result)
        else:
            rospy.loginfo("Goal Failed. Aborting.")
            goal_handle.set_aborted(result)

        rospy.loginfo("Goal ID: " + str(goal_id.id) + " Goal Processing Done.")


    # This function will be called when Goal Cancel request is send to the Action Server
    def on_cancel(self, goal_handle):
        """
        This function is invoked if the thien is a cancel request from the action client
        and cancels them.

        Parmaters:
            goal_handle (object): This is the goal handle coorespnding to the goal that has
                been requested to be cancelled by the action client
        """
        rospy.loginfo("Received cancel request.")
        print goal_handle

# Main
def main():
    """
    Main function that intilizes the class and the node
    """
    rospy.init_node('node_action_server_ros_iot_bridge')

    action_server = IotRosBridgeActionServer()

    rospy.spin()



if __name__ == '__main__':
    main()
