import erdos
import time
import heapq
from erdos.operators import map
from enum import Enum


class Marking(Enum):
    PROD = 1
    PRIM = 2
    SEC = 3


class Status(Enum):
    ACTIVE = 1
    DEAD = 2
    STDBY = 3
    PAUSE = 4


class Conn(Enum):
    SEND = 1
    RECV = 2
    ACK = 3
    PAUSE = 4


def clear_queue_send(queue, timestamp, write_stream=None, sender="Operator"):
    """
    Clears queue of all messages <= timestamp and sends each message.
    If a write_stream is passed in, every message removed from the
    queue will also be sent.
    """
    while queue and queue[0][0] <= timestamp:
        msg = heapq.heappop(queue)
        if write_stream is not None:
            print("{sender}: sending {msg}".format(sender=sender, msg=msg))
            write_stream.send(msg)


class DestOp(erdos.Operator):
    """ 1 in 1 out operator

        In1 - Receives data from EgressOp
        Out1 - Sends watermark to EgressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_watermark_stream: erdos.WriteStream):
        read_stream.add_callback(DestOp.callback, [write_watermark_stream])

    @staticmethod
    def callback(msg, write_watermark_stream):
        print("DestOp: received {msg}".format(msg=msg))
        watermark = erdos.WatermarkMessage(msg.timestamp)
        print("DestOp: sending watermark {watermark}".format(
            watermark=watermark))
        write_watermark_stream.send(watermark)

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]


class SourceOp(erdos.Operator):
    """ 1 out operator

        Out1 - Sends data to IngressOp
    """
    def __init__(self, write_stream: erdos.WriteStream):
        self.write_stream = write_stream

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = 0
        while True:
            timestamp = erdos.Timestamp(coordinates=[count])
            payload = [erdos.Message(timestamp, count)] * 2
            msg = erdos.Message(timestamp, payload)
            print("SourceOp: sending {msg}".format(msg=msg))
            self.write_stream.send(msg)
            watermark = erdos.WatermarkMessage(msg.timestamp)
            print(
                "SourceOp: sending watermark {watermark} to IngressOp".format(
                    watermark=watermark))
            self.write_stream.send(watermark)
            count += 1
            time.sleep(1)


class IngressOp(erdos.Operator):
    """ 3 in 1 out operator

        In1 - Receives data from SourceOp
        In2 - Receives watermark from SConsPOp (Primary consumer op)
        In3 - Receives watermark from SConsSOp (Secondary consumer op)
        Out1 - Sends data to SConsPOp and SConsSOp (primary and consumer ops)
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 primary_cons_watermark_stream: erdos.ReadStream,
                 secondary_cons_watermark_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream):
        self.queue = []
        # Gets watermarks from SourceOp
        read_stream.add_watermark_callback(
            self.src_watermark_callback, [write_stream])
        # Gets data messages from SourceOp
        read_stream.add_callback(self.callback)
        erdos.add_watermark_callback(
            [primary_cons_watermark_stream, secondary_cons_watermark_stream],
            [], self.ack_watermark_callback)

    @staticmethod
    def connect(read_stream,
                primary_cons_watermark_stream,
                secondary_cons_watermark_stream):
        return [erdos.WriteStream()]

    def callback(self, msg):
        print("IngressOp: received message {msg}".format(msg=msg))
        heapq.heappush(self.queue, (msg.timestamp, msg))

    def src_watermark_callback(self, timestamp: erdos.Timestamp, write_stream: erdos.WriteStream):
        # After getting watermark from SourceOp, sends data to consumers
        print("IngressOp: received watermark at timestamp {timestamp}".format(
            timestamp=timestamp))
        i = 0
        while i < len(self.queue) and self.queue[i][0] <= timestamp:
            msg = self.queue[i][1]
            print("IngressOp: sending {msg}".format(msg=msg))
            write_stream.send(msg)
            i += 1

    def ack_watermark_callback(self, timestamp):
        print(
            "IngressOp: received primary & secondary cons watermark at timestamp: {timestamp}"
            .format(timestamp=timestamp))
        print("BEFORE QUEUE", self.queue)
        # Clears queue of messages <= timestamp
        clear_queue_send(self.queue, timestamp)
        print("AFTER QUEUE", self.queue)


class SConsOp(erdos.Operator):
    """ 1 in 2 out operator

        In1 - Receives data from IngressOp
        Out1 - Sends data to SProdPOp
        Out2 - Sends watermark to IngressOp
    """
    def __init__(self, data_stream: erdos.ReadStream,
                 write_data_stream: erdos.WriteStream,
                 write_watermark_stream: erdos.WriteStream):
        data_stream.add_callback(self.data_callback, [write_data_stream])
        self._write_watermark_stream = write_watermark_stream
        self.queue = {}

    @staticmethod
    def connect(data_stream):
        write_data_stream = erdos.WriteStream()
        write_watermark_stream = erdos.WriteStream()
        return [write_data_stream, write_watermark_stream]

    def data_callback(self, msg, write_stream):
        print("SConsOp: received {msg}".format(msg=msg))
        watermark = erdos.WatermarkMessage(msg.timestamp)
        print("SConsOp: sending watermark {watermark} to IngressOp".format(
            watermark=watermark))
        self._write_watermark_stream.send(watermark)
        print("SConsOp: sending {msg} to SProdOp".format(msg=msg))
        write_stream.send(msg)


class SProdPOp(erdos.Operator):
    """ 1 in 1 out operator

        In1 - Receives data from SConsPOp (primary consumer op)
        Out1 - Sends data to EgressOp
    """
    def __init__(self, data_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream):
        data_stream.add_callback(SProdPOp.data_callback, [write_stream])

    @staticmethod
    def connect(data_stream):
        return [erdos.WriteStream()]

    @staticmethod
    def data_callback(msg, write_stream):
        print("SProdPOp: received {msg}".format(msg=msg))
        print("SProdPOp: sending {msg}".format(msg=msg))
        write_stream.send(msg)


class SProdSOp(erdos.Operator):
    """ 2 in operator

        In1 - Receives data from SConsSOp (Secondary consumer op)
        In2 - Receives watermark from EgressOp
    """
    def __init__(self, data_stream: erdos.ReadStream,
                 egress_watermark_stream: erdos.ReadStream):
        self.queue = {}
        data_stream.add_callback(self.data_callback)
        egress_watermark_stream.add_watermark_callback(
            self.egress_watermark_callback)

    @staticmethod
    def connect(data_stream, egress_watermark_stream):
        return []

    def data_callback(self, msg):
        # Queue up the data received in the SProdSOp and order by timestamp
        self.queue[msg.timestamp] = msg
        print("SProdSOp: received {msg}".format(msg=msg))

    def egress_watermark_callback(self, timestamp):
        # Clear the queue with timestamps <= timestamp
        print("SProdSOp: received watermark from EgressOp")
        print("SProdSOp: before queue: {queue}".format(queue=self.queue))
        self.queue = {
            key: val
            for key, val in self.queue.items() if key > timestamp
        }
        print("SProdSOp: after queue: {queue}".format(queue=self.queue))


class EgressOp(erdos.Operator):
    """ 2 in 2 out operator

        In1 - Receives data from SProdPOp
        In2 - Receives watermark from DestOp
        Out1 - Sends watermark to SProdSOp
        Out2 - Sends data to DestOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 watermark_stream: erdos.ReadStream,
                 write_watermark_stream: erdos.WriteStream,
                 write_stream: erdos.WriteStream):
        read_stream.add_callback(self.callback,
                                 [write_stream, write_watermark_stream])
        watermark_stream.add_watermark_callback(self.dest_watermark_callback)
        self.queue = {}
        # self.delete = {Marking.PROD, Marking.}

    @staticmethod
    def connect(read_stream, watermark_stream):
        write_stream = erdos.WriteStream()
        write_watermark_stream = erdos.WriteStream()
        return [write_watermark_stream, write_stream]

    def callback(self, msg, write_stream, write_watermark_stream):
        print("EgressOp: received {msg} from primary pipe".format(msg=msg))
        self.queue[msg.timestamp] = msg

        # Sends a watermark message to the SProdSOp
        watermark = erdos.WatermarkMessage(msg.timestamp)
        print("EgressOp: sending {watermark} to SProdS".format(
            watermark=watermark))
        write_watermark_stream.send(watermark)

        print("EgressOp: sending {msg} to DestOp".format(msg=msg))
        write_stream.send(msg)

    def dest_watermark_callback(self, timestamp):
        # Clear the queue with timestamps <= timestamp
        print("EgressOp: received watermark from destination")
        print("EgressOp: before queue: {queue}".format(queue=self.queue))
        self.queue = {
            key: val
            for key, val in self.queue.items() if key > timestamp
        }
        print("EgressOp: after queue: {queue}".format(queue=self.queue))


def main():
    def add(msg):
        """Mapping Function passed into MapOp,
           returns a new Message that sums the data of each message in
           msg.data."""
        total = 0
        print("MapOp: received {msg}".format(msg=msg))
        for i in msg.data:
            total += i.data
        return erdos.Message(msg.timestamp, total)

    # Data comes out of SourceOp
    (source_stream, ) = erdos.connect(SourceOp,
                                      erdos.OperatorConfig(name="SourceOp"),
                                      [])

    primary_cons_watermark_stream = erdos.LoopStream()
    secondary_cons_watermark_stream = erdos.LoopStream()

    # Data flows into IngressOp and out of IngressOp again
    (ingress_stream, ) = erdos.connect(
        IngressOp, erdos.OperatorConfig(name="IngressOp"), [
            source_stream, primary_cons_watermark_stream,
            secondary_cons_watermark_stream
        ])

    # Data comes into primary SConsOp and out of primary SConsOp
    (primary_cons_stream, primary_watermark_stream) = erdos.connect(
        SConsOp, erdos.OperatorConfig(name="SConsPOp"), [ingress_stream])

    primary_cons_watermark_stream.set(primary_watermark_stream)

    # Data comes into secondary SConsOp and out of secondary SConsOp
    (secondary_cons_stream, secondary_watermark_stream) = erdos.connect(
        SConsOp, erdos.OperatorConfig(name="SConsSOp"), [ingress_stream])

    secondary_cons_watermark_stream.set(secondary_watermark_stream)

    # # Data from IngressOp runs intermediate primary Map Operator and sends
    # # modified data out to SProdPOp
    # (primary_map_stream, ) = erdos.connect(
    #     map.Map,
    #     erdos.OperatorConfig(name="Primary MapOp"), [primary_cons_stream],
    #     function=add)

    # # Data from IngressOp runs intermediate secondary Map Operator and sends
    # # modified data out to SProdSOp
    # (secondary_map_stream, ) = erdos.connect(
    #     map.Map,
    #     erdos.OperatorConfig(name="Secondary MapOp"), [secondary_cons_stream],
    #     function=add)

    # # Data from intermediate operators and goes into SProdPOp
    # # and sends data out to EgressOp
    # (primary_prod_stream, ) = erdos.connect(
    #     SProdPOp, erdos.OperatorConfig(name="SProdPOp"), [primary_map_stream])

    # # Data from Ingress passed into EgressOp and EgressOp
    # # returns WatermarkMessage
    # dest_watermark_stream = erdos.LoopStream()
    # (egress_watermark_stream, egress_dest_stream) = erdos.connect(
    #     EgressOp, erdos.OperatorConfig(name="EgressOp"),
    #     [primary_prod_stream, dest_watermark_stream])

    # # EgressOp sends Watermark to SProdSOp to clear its queue of messages.
    # # Also SProdSOp receives data from secondary intermediate operators
    # erdos.connect(SProdSOp, erdos.OperatorConfig(name="SProdSOp"),
    #               [secondary_map_stream, egress_watermark_stream])

    # # Data from EgressOp flows into DestOp and DestOp
    # # sends WatermarkMessage back to EgressOp to clear its queue
    # (watermark_stream, ) = erdos.connect(DestOp,
    #                                      erdos.OperatorConfig(name="DestOp"),
    #                                      [egress_dest_stream])
    # dest_watermark_stream.set(watermark_stream)

    # stream = erdos.ExtractStream(primary_cons_watermark_stream)

    erdos.run(graph_filename="graph.gv")

    # while True:
    #     print("ExtractStream: received {recv_msg}".format(recv_msg=stream.read()))

    # Need to maintain buffer for acks in case they come before data is received in SProdS from secondary pipeline

    # Buffer B : { {sn, tuple, mark}, … }
    # delete = { Marking.PROD | Marking.PRIM | Marking.SEC }
    # status[Marking.PRIM] = { Status.ACTIVE,Status.DEAD,Status.STDBY,Status.PAUSE }
    # conn[Marking.PRIM] = { Conn.SEND | Conn.RECV | Conn.ACK | Conn.PAUSE }
    # dest = { Marking.PRIM, Marking.SEC }


if __name__ == "__main__":
    main()
