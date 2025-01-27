# kafka_client.py

import sys
import Ice
import confluent_kafka
import json
import remotetypes as rt

class KafkaClient(Ice.Application):
    """
    Main Kafka client class that inherits from Ice.Application.
    This class manages reading the configuration, setting up the 
    remote factory proxy, creating the Kafka consumer and producer, 
    and finally delegating message processing to a separate function.
    """
    def run(self, argv):
        """
        The run method is automatically called by Ice.Application
        after initializing the communicator. This is where we:
          1. Load our configuration from 'config.json'.
          2. Obtain the remote factory proxy (using ICE).
          3. Create our Kafka consumer and producer.
          4. Delegate message consumption and processing to 
             'consume_and_process_messages'.
          5. Handle KeyboardInterrupt signals and ensure that the 
             consumer is closed before exiting.
        """
        # 1. Read the configuration file (config.json)
        with open('config.json') as f:
            config = json.load(f)

        # 2. Create a proxy to the remote server (Factory). 
        #    This uses ICE's communicator and the 'ice_proxy_string' 
        #    from the config file to connect to the appropriate service.
        proxy = self.communicator().stringToProxy(config['ice_proxy_string'])
        factory = rt.RemoteTypes.FactoryPrx.checkedCast(proxy)

        # 3. Create the consumer and producer for Kafka.
        #    These are set up using helper functions defined below.
        consumer = create_kafka_consumer(config)
        producer = create_kafka_producer(config)

        # 4. Delegate the logic of consuming messages from Kafka 
        #    and processing them (including sending responses) 
        #    to a separate function.
        try:
            consume_and_process_messages(consumer, producer, factory, config)
        except KeyboardInterrupt:
            # This will be triggered if the user presses Ctrl + C 
            # (or an equivalent interrupt signal).
            pass
        finally:
            # Ensure that the consumer is properly closed before exiting.
            consumer.close()

        # Return 0 to signal that the application terminated successfully.
        return 0


def create_kafka_consumer(config):
    """
    Creates and returns a confluent_kafka.Consumer object using the settings 
    from the given config dictionary. This method:
      - Reads the bootstrap servers from config['kafka_bootstrap_servers'].
      - Uses a group ID from config['kafka_group_id'].
      - Specifies an auto offset reset policy ('earliest' by default).
      - Subscribes the consumer to the topic listed in config['kafka_input_topic'].

    :param config: A dictionary containing the Kafka connection parameters.
    :return: A confluent_kafka.Consumer object ready to poll messages.
    """
    consumer = confluent_kafka.Consumer({
        'bootstrap.servers': config['kafka_bootstrap_servers'],
        'group.id': config['kafka_group_id'],
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([config['kafka_input_topic']])
    return consumer


def create_kafka_producer(config):
    """
    Creates and returns a confluent_kafka.Producer object using the settings 
    from the given config dictionary. This method reads the bootstrap servers 
    from config['kafka_bootstrap_servers'] to set up the Kafka producer.

    :param config: A dictionary containing the Kafka connection parameters.
    :return: A confluent_kafka.Producer object ready to produce messages.
    """
    return confluent_kafka.Producer({
        'bootstrap.servers': config['kafka_bootstrap_servers']
    })


def consume_and_process_messages(consumer, producer, factory, config):
    """
    The main loop responsible for consuming messages from Kafka, processing 
    them, and then publishing a response message. This function does the 
    following:
      1. Continuously polls the consumer for new messages.
      2. Checks for any errors. If an error is found, raises a KafkaException.
      3. When a valid message is received, it is assumed to contain JSON 
         representing one or more operations (event_list).
      4. For each operation (or 'op'), the function 'hacer_evento' is called 
         to carry out the remote operation via the factory proxy.
      5. The results of each operation are collected into a 'message' list 
         containing status, error messages, and/or result values.
      6. This 'message' list is then serialized to JSON and published back 
         to the Kafka output topic specified in config['kafka_output_topic'].

    :param consumer: confluent_kafka.Consumer object used for reading messages.
    :param producer: confluent_kafka.Producer object used for sending messages.
    :param factory: An ICE proxy factory that allows creation of proxies 
                    to remote RList, RDict, RSet, etc.
    :param config: A dictionary holding all relevant Kafka and ICE configuration.
    """
    while True:
        # Attempt to read a message from the consumer.
        msg = consumer.poll(timeout=1.0)
        
        # If no message is available, just continue the loop.
        if msg is None:
            continue
        
        # If there's an error in the message, raise an exception.
        if msg.error():
            raise confluent_kafka.KafkaException(msg.error())
        else:
            try:
                event_list = json.loads(msg.value().decode())
            except json.JSONDecodeError:
                print(f"Received invalid JSON: {msg.value().decode()}")
                continue

            # Initialize a list to hold the results for all operations found in this message.
            message = []
            for op in event_list:
                # Call the 'hacer_evento' function, which will dispatch 
                # the correct operation to the remote object.
                result, error = hacer_evento(factory, op)
                
                # Build the response for each operation:
                if error:
                    if op.get("id") is not None:
                        message.append({"id": op["id"], "status": "error", "error": error})

                elif result is not None:
                    # If the operation succeeded and returned a non-null result, include it.
                    message.append({"id": op["id"], "status": "ok", "result": result})
                else:
                    # If the operation succeeded but did not return a result, omit the 'result' key.
                    message.append({"id": op["id"], "status": "ok"})
                    
            if message:
                # Publish the response to the output topic and ensure 
                # the message is flushed to Kafka.
                producer.produce(config['kafka_output_topic'], value=json.dumps(message))
                producer.flush()


def get_rtype_proxy(factory, obj_type, obj_identifier):
    """
    Retrieves the specific remote proxy (RList, RDict, RSet, etc.) from the 
    provided factory based on the 'obj_type'. If the 'obj_type' is not recognized, 
    this function will return None.

    :param factory: The ICE remote factory proxy that can return specific 
                    object proxies (e.g., RList, RDict, RSet).
    :param obj_type: A string that indicates the type of remote object we want 
                     (e.g., "RList", "RDict", or "RSet").
    :return: An ICE proxy for the requested remote object, or None if the 
             type is not recognized.
    """
    if obj_type == "RList":
        return rt.RemoteTypes.RListPrx.checkedCast(
            factory.get(rt.RemoteTypes.TypeName.RList, obj_identifier)
        )
    elif obj_type == "RDict":
        return rt.RemoteTypes.RDictPrx.checkedCast(
            factory.get(rt.RemoteTypes.TypeName.RDict, obj_identifier)
        )
    elif obj_type == "RSet":
        return rt.RemoteTypes.RSetPrx.checkedCast(
            factory.get(rt.RemoteTypes.TypeName.RSet, obj_identifier)
        )
    else:
        # If we do not recognize the object type, return None 
        # so the caller can handle it as an error.
        return None


def process_common_operation(rtype_prx, operation, args):
    """
    Executes the operations that are shared among multiple remote object 
    types (RList, RDict, RSet). These include:
      - identifier
      - remove
      - length
      - contains
      - hash
      - iter (which is not supported in this example)

    If an operation is not recognized here, this function returns (None, None), 
    indicating that the operation might be specific to a particular object type 
    and should be handled elsewhere.

    :param rtype_prx: A proxy object to the remote RList, RDict, or RSet.
    :param operation: The name of the operation (a string).
    :param args: A dictionary containing additional arguments needed 
                 by the operation (e.g., "item", "index", etc.).
    :return: A tuple (result, error). If the operation produces a result, 
             'result' will be a non-None value. If the operation fails or 
             is unsupported, 'error' will contain a non-None string.
    """
    if operation == "identifier":
        return rtype_prx.identifier(), None
    elif operation == "remove":
        # 'remove' requires an 'item' argument to remove from the remote object.
        rtype_prx.remove(args["item"])
        return None, None
    elif operation == "length":
        return rtype_prx.length(), None
    elif operation == "contains":
        # 'contains' requires an 'item' argument to check for membership.
        return rtype_prx.contains(args["item"]), None
    elif operation == "hash":
        return rtype_prx.hash(), None
    elif operation == "iter":
        # For demonstration, we assume 'iter' is not supported here.
        return None, "OperationNotSupported"

    # If the operation is not one of the above, return (None, None) 
    # so the caller can process it as a specialized operation.
    return None, None


def process_rlist(rtype_prx, operation, args):
    """
    Handles operations specific to RList objects. 
    These include 'append', 'pop', and 'getItem'.

    :param rtype_prx: A proxy to an RList remote object.
    :param operation: Name of the operation to perform.
    :param args: Arguments needed by the operation, such as "item" or "index".
    :return: A tuple (result, error) indicating the result of the operation 
             or any error encountered.
    """
    result, error = None, None
    if operation == "append":
        rtype_prx.append(args["item"])
    elif operation == "pop":
        # 'pop' may optionally require an 'index'.
        if "index" in args:
            rtype_prx.pop(int(args["index"]))
        else:
            rtype_prx.pop()
    elif operation == "getItem":
        # 'getItem' needs an 'index' to retrieve the element at that position.
        result = rtype_prx.getItem(int(args["index"]))
    return result, error


def process_rdict(rtype_prx, operation, args):
    """
    Handles operations specific to RDict objects.
    These include 'setItem', 'getItem', and 'pop'.

    :param rtype_prx: A proxy to an RDict remote object.
    :param operation: Name of the operation to perform.
    :param args: Arguments such as 'key' and 'item' for dict manipulations.
    :return: A tuple (result, error) indicating the result of the operation 
             or any error encountered.
    """
    result, error = None, None
    if operation == "setItem":
        # 'setItem' requires both 'key' and 'item'.
        rtype_prx.setItem(args["key"], args["item"])
    elif operation == "getItem":
        # 'getItem' requires a 'key' to retrieve the corresponding value.
        result = rtype_prx.getItem(args["key"])
    elif operation == "pop":
        # 'pop' in RDict requires a 'key' to remove from the dictionary.
        rtype_prx.pop(args["key"])
    return result, error


def process_rset(rtype_prx, operation, args):
    """
    Handles operations specific to RSet objects.
    These include 'add' and 'pop'.

    :param rtype_prx: A proxy to an RSet remote object.
    :param operation: Name of the operation to perform.
    :param args: Arguments such as 'item' which might be required for 'add'.
    :return: A tuple (result, error) indicating the result of the operation 
             or any error encountered.
    """
    result, error = None, None
    if operation == "add":
        # 'add' requires an 'item' argument to insert into the set.
        rtype_prx.add(args["item"])
    elif operation == "pop":
        # 'pop' removes and returns an arbitrary element from the set 
        # (though the remote method may handle it differently).
        rtype_prx.pop()
    return result, error


def hacer_evento(factory, event):
    """
    Processes a single event (i.e., a single operation request on a 
    remote object). It determines which remote object type is being 
    targeted (RList, RDict, or RSet), checks whether the operation is 
    a common one or requires specialized handling, and then executes 
    the operation accordingly.

    :param factory: The ICE remote factory object used to retrieve proxies.
    :param event: A dictionary describing the requested operation, expected 
                  to contain:
                      "id": A unique identifier for the request
                      "object_type": "RList", "RDict", or "RSet"
                      "operation": Name of the operation (e.g., "append")
                      "args": A dictionary of arguments (optional)
    :return: A tuple (result, error) where 'result' can be any object/str 
             returned by the operation, and 'error' is a string describing 
             an error if one occurs, or None otherwise.
    """

    if not all(key in event for key in ["id", "object_type", "object_identifier", "operation"]):
        return None, "MalformedOperation"

    obj_type = event["object_type"]
    obj_identifier = event["object_identifier"]
    operation = event["operation"]
    args = event.get("args", {})

    # 1. Obtain the appropriate proxy using the factory based on the object type.
    rtype_prx = get_rtype_proxy(factory, obj_type, obj_identifier)
    if rtype_prx is None:
        # If obj_type is unrecognized, return an error.
        return None, "UnknownObjectType"

    # 2. Attempt to process the operation in the "common" group.
    #    If 'process_common_operation' returns a non-None result or an error,
    #    we immediately return that to the caller.
    result, error = process_common_operation(rtype_prx, operation, args)
    if result is not None or error is not None:
        return result, error

    # 3. If not handled as a common operation, dispatch to specialized logic 
    #    based on the object type (RList, RDict, or RSet).
    if obj_type == "RList":
        result, error = process_rlist(rtype_prx, operation, args)
    elif obj_type == "RDict":
        result, error = process_rdict(rtype_prx, operation, args)
    elif obj_type == "RSet":
        result, error = process_rset(rtype_prx, operation, args)
    else:
        # This else clause should not occur if 'get_rtype_proxy' 
        # handles all recognized types, but is here for safety.
        error = "UnknownObjectType"

    return result, error


def main():
    """
    The main entry point for this script. Instantiates the Kafka_client class
    and invokes the 'main' method from Ice.Application. This ensures that 
    all ICE-related setup (e.g. parsing command-line arguments, initializing 
    the communicator) occurs before we execute our custom logic in 'run'.
    """
    client = KafkaClient()
    sys.exit(client.main(sys.argv))


if __name__ == '__main__':
    main()
