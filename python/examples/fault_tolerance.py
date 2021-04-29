import erdos
import time
from collections import deque
from erdos.operators import map
from utils import Marking, Status, Conn


def clear_queue_send(queue,
                     max_timestamp,
                     write_downstream=None,
                     write_upstream=None,
                     sender="Operator"):
    """
    Clears queue of all messages <= timestamp and sends each message.
    If a write_stream is passed in, every message removed from the
    queue will also be sent.
    """
    while queue and queue[0][0] <= max_timestamp:        
        data = queue.popleft()
        ts, msg = data[0], data[1]
        if write_downstream is not None:
            # Send data to next operators along with watermark
            print("{name}: sending {msg}".format(name=sender, msg=msg))
            write_downstream.send(msg)
            watermark = erdos.WatermarkMessage(ts)
            print("{name}: sending watermark {watermark} downstream".format(
                name=sender, watermark=watermark))
            write_downstream.send(watermark)
        if write_upstream is not None:
            # Send watermark acks back to previous operator
            watermark = erdos.WatermarkMessage(ts)
            print("{name}: sending watermark {watermark} upstream".format(
                name=sender, watermark=watermark))
            write_upstream.send(watermark)


class DestOp(erdos.Operator):
    """ 1 in 1 out operator

        In1 - Receives data + watermark from EgressOp
        Out1 - Sends watermark to EgressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_watermark_stream: erdos.WriteStream):
        self.queue = deque([])
        read_stream.add_callback(self.callback)
        read_stream.add_watermark_callback(self.watermark_callback,
                                           [write_watermark_stream])

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

    def callback(self, msg):
        print("{name}: received {msg}".format(msg=msg, name=self.config.name))
        self.queue.append((msg.timestamp, msg))

    def watermark_callback(self, timestamp, write_watermark_stream):
        watermark = erdos.WatermarkMessage(timestamp)
        print("{name}: sending watermark {watermark} to EgressOp".format(
            watermark=watermark, name=self.config.name))
        write_watermark_stream.send(watermark)


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
            print("{name}: sending {msg}".format(msg=msg,
                                                 name=self.config.name))
            self.write_stream.send(msg)
            watermark = erdos.WatermarkMessage(msg.timestamp)
            print("{name}: sending watermark {watermark} to IngressOp".format(
                watermark=watermark, name=self.config.name))
            self.write_stream.send(watermark)
            count += 1
            time.sleep(1)


class IngressOp(erdos.Operator):
    """ 3 in 1 out operator

        In1 - Receives data + watermark from SourceOp
        In2 - Receives watermark from SConsPOp (Primary consumer op)
        In3 - Receives watermark from SConsSOp (Secondary consumer op)
        Out1 - Sends data + watermark to SConsPOp and SConsSOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 primary_cons_watermark_stream: erdos.ReadStream,
                 secondary_cons_watermark_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream):
        self.queue = deque([])
        self.conn = dict.fromkeys([Marking.PRIM, Marking.SEC],
                                  {Conn.SEND, Conn.ACK})
        self.delete = {Marking.PROD, Marking.PRIM, Marking.SEC}
        self.dest = Marking.PRIM

        # Gets watermarks from SourceOp
        read_stream.add_watermark_callback(self.src_watermark_callback,
                                           [write_stream])
        # Gets data messages from SourceOp
        read_stream.add_callback(self.callback)
        erdos.add_watermark_callback(
            [primary_cons_watermark_stream, secondary_cons_watermark_stream],
            [], self.ack_watermark_callback)

    @staticmethod
    def connect(read_stream, primary_cons_watermark_stream,
                secondary_cons_watermark_stream):
        return [erdos.WriteStream()]

    def callback(self, msg):
        print("{name}: received message {msg}".format(msg=msg,
                                                      name=self.config.name))
        # This message was produced from below, so its marking is set to Marking.PROD
        self.queue.append((msg.timestamp, msg, {Marking.PROD}))

    def src_watermark_callback(self, timestamp: erdos.Timestamp,
                               write_stream: erdos.WriteStream):
        # After getting watermark from SourceOp, sends data to consumers
        print("{name}: received watermark at timestamp {timestamp}".format(
            timestamp=timestamp, name=self.config.name))
        i = 0
        while i < len(self.queue) and self.queue[i][0] <= timestamp:
            msg = self.queue[i][1]
            print("{name}: sending {msg} to SConsOp".format(
                msg=msg, name=self.config.name))
            write_stream.send(msg)
            watermark = erdos.WatermarkMessage(msg.timestamp)
            print("{name}: sending watermark {watermark} to SConsOp".format(
                watermark=watermark, name=self.config.name))
            write_stream.send(watermark)
            i += 1

    def ack_watermark_callback(self, timestamp):
        print("{name}: received primary & secondary watermarks at {timestamp}".
              format(timestamp=timestamp, name=self.config.name))
        # Clears queue of messages <= timestamp
        print("QUEUE BEFORE", self.queue)
        clear_queue_send(self.queue, timestamp)
        print("QUEUE AFTER", self.queue)


class SConsOp(erdos.Operator):
    """ 1 in 2 out operator

        In1 - Receives data + watermark from IngressOp
        Out1 - Sends data + watermark to SProdPOp
        Out2 - Sends watermark to IngressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream,
                 write_watermark_stream: erdos.WriteStream):
        self.queue = deque([])
        # Gets watermarks from IngressOp
        read_stream.add_watermark_callback(
            self.watermark_callback, [write_stream, write_watermark_stream])
        # Gets data messages from IngressOp
        read_stream.add_callback(self.callback)

    @staticmethod
    def connect(read_stream):
        write_stream = erdos.WriteStream()
        write_watermark_stream = erdos.WriteStream()
        return [write_stream, write_watermark_stream]

    def callback(self, msg):
        print("{name}: received {msg}".format(msg=msg, name=self.config.name))
        self.queue.append((msg.timestamp, msg))

    def watermark_callback(self, timestamp, write_stream,
                           write_watermark_stream):
        print(self.config.name + " QUEUE BEFORE " + str(timestamp), self.queue)
        clear_queue_send(self.queue,
                         timestamp,
                         write_stream,
                         write_watermark_stream,
                         sender=self.config.name)
        print(self.config.name + " QUEUE AFTER " + str(timestamp), self.queue)


class SProdPOp(erdos.Operator):
    """ 1 in 1 out operator

        In1 - Receives data + watermark from SConsPOp (primary consumer op)
        Out1 - Sends data + watermark to EgressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream):
        self.queue = deque([])
        self.conn = {}
        self.conn[Marking.PRIM] = Conn.SEND
        self.delete = {Marking.PRIM, Marking.SEC}
        self.dest = Marking.PRIM
        # Gets watermarks from SConsPOp
        read_stream.add_watermark_callback(self.watermark_callback,
                                           [write_stream])
        # Gets data messages from SConsPOp
        read_stream.add_callback(self.callback)

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

    def callback(self, msg):
        print("{name}: received {msg}".format(msg=msg, name=self.config.name))
        self.queue.append((msg.timestamp, msg))

    def watermark_callback(self, timestamp, write_stream):
        print("{name}: received watermark at timestamp {ts} from SConsPOp".
              format(ts=timestamp, name=self.config.name))
        print(self.config.name + " QUEUE BEFORE", self.queue)
        clear_queue_send(self.queue,
                         timestamp,
                         write_downstream=write_stream,
                         sender=self.config.name)
        print(self.config.name + " QUEUE AFTER", self.queue)


class SProdSOp(erdos.Operator):
    """ 2 in operator

        In1 - Receives data + watermark from SConsSOp (Secondary consumer op)
        In2 - Receives watermark from EgressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 egress_watermark_stream: erdos.ReadStream):
        self.queue = deque([])
        self.conn = {}
        self.conn[Marking.PRIM] = Conn.ACK  # data is not forwarded
        self.delete = {Marking.PRIM, Marking.SEC}
        self.dest = Marking.PRIM
        read_stream.add_callback(self.callback)
        read_stream.add_watermark_callback(self.watermark_callback)
        egress_watermark_stream.add_watermark_callback(
            self.egress_watermark_callback)

    @staticmethod
    def connect(read_stream, egress_watermark_stream):
        return []

    def callback(self, msg):
        # Queue up the data received in the SProdSOp and order by timestamp
        print("{name}: received {msg}".format(msg=msg, name=self.config.name))
        self.queue.append((msg.timestamp, msg))

    def watermark_callback(self, timestamp):
        print("{name}: received watermark at {ts} from SConsSOp".format(
            name=self.config.name, ts=timestamp))

    def egress_watermark_callback(self, timestamp):
        # Clear the queue with timestamps <= timestamp
        print("{name}: received watermark from EgressOp".format(
            name=self.config.name))
        print(self.config.name + " QUEUE BEFORE", self.queue)
        clear_queue_send(self.queue, timestamp)
        print(self.config.name + " QUEUE AFTER", self.queue)


class EgressOp(erdos.Operator):
    """ 2 in 2 out operator

        In1 - Receives data + watermark from SProdPOp
        In2 - Receives watermark from DestOp
        Out1 - Sends watermark to SProdSOp
        Out2 - Sends data + watermark to DestOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 dest_watermark_stream: erdos.ReadStream,
                 write_watermark_stream: erdos.WriteStream,
                 write_stream: erdos.WriteStream):
        self.queue = deque([])
        read_stream.add_callback(self.callback)
        read_stream.add_watermark_callback(
            self.watermark_callback, [write_stream, write_watermark_stream])
        dest_watermark_stream.add_watermark_callback(
            self.dest_watermark_callback)
        # self.delete = {Marking.PROD, Marking.}

    @staticmethod
    def connect(read_stream, dest_watermark_stream):
        write_stream = erdos.WriteStream()
        write_watermark_stream = erdos.WriteStream()
        return [write_watermark_stream, write_stream]

    def callback(self, msg):
        print("{name}: received {msg} from SProdPOp".format(
            msg=msg, name=self.config.name))
        self.queue.append((msg.timestamp, msg))

    def watermark_callback(self, timestamp, write_stream,
                           write_watermark_stream):
        i = 0
        print(
            "{name}: received watermark at timestamp {timestamp} from SProdPOp"
            .format(timestamp=timestamp, name=self.config.name))
        print("EGRESS OP: BEFORE QUEUE", self.queue)
        while i < len(self.queue) and self.queue[i][0] <= timestamp:
            msg = self.queue[i][1]
            print("{name}: sending {msg} to DestOp".format(
                msg=msg, name=self.config.name))
            write_stream.send(msg)
            watermark = erdos.WatermarkMessage(msg.timestamp)
            print("{name}: sending watermark {watermark} to DestOp".format(
                watermark=watermark, name=self.config.name))
            write_stream.send(watermark)
            # Sends a watermark message to the SProdSOp
            watermark = erdos.WatermarkMessage(timestamp)
            print("{name}: sending {watermark} to SProdSOp".format(
                watermark=watermark, name=self.config.name))
            write_watermark_stream.send(watermark)
            i += 1

    def dest_watermark_callback(self, timestamp):
        # Clear the queue with timestamps <= timestamp
        print(
            "{name}: received watermark at timestamp {timestamp} from DestOp".
            format(timestamp=timestamp, name=self.config.name))
        clear_queue_send(self.queue, timestamp)


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

    # Data from IngressOp runs intermediate primary Map Operator and sends
    # modified data out to SProdPOp
    (primary_map_stream, ) = erdos.connect(
        map.Map,
        erdos.OperatorConfig(name="Primary MapOp"), [primary_cons_stream],
        function=add)

    # Data from IngressOp runs intermediate secondary Map Operator and sends
    # modified data out to SProdSOp
    (secondary_map_stream, ) = erdos.connect(
        map.Map,
        erdos.OperatorConfig(name="Secondary MapOp"), [secondary_cons_stream],
        function=add)

    # Data from intermediate operators and goes into SProdPOp
    # and sends data out to EgressOp
    (primary_prod_stream, ) = erdos.connect(
        SProdPOp, erdos.OperatorConfig(name="SProdPOp"), [primary_map_stream])

    # Data from Ingress passed into EgressOp and EgressOp
    # returns WatermarkMessage
    dest_watermark_stream = erdos.LoopStream()
    (egress_watermark_stream, egress_dest_stream) = erdos.connect(
        EgressOp, erdos.OperatorConfig(name="EgressOp"),
        [primary_prod_stream, dest_watermark_stream])

    # EgressOp sends Watermark to SProdSOp to clear its queue of messages.
    # Also SProdSOp receives data from secondary intermediate operators
    erdos.connect(SProdSOp, erdos.OperatorConfig(name="SProdSOp"),
                  [secondary_map_stream, egress_watermark_stream])

    # Data from EgressOp flows into DestOp and DestOp
    # sends WatermarkMessage back to EgressOp to clear its queue
    (watermark_stream, ) = erdos.connect(DestOp,
                                         erdos.OperatorConfig(name="DestOp"),
                                         [egress_dest_stream])
    dest_watermark_stream.set(watermark_stream)

    # stream = erdos.ExtractStream(primary_cons_watermark_stream)

    erdos.run(graph_filename="graph.gv")

    # while True:
    #     print("{name}: received {recv_msg}".format(recv_msg=stream.read()))

    # Need to maintain buffer for acks in case they come before data is
    # received in SProdS from secondary pipeline

    # Buffer B : { {sn, tuple, mark}, … }
    # delete = { Marking.PROD | Marking.PRIM | Marking.SEC }
    # status[Marking.PRIM] = { Status.ACTIVE,Status.DEAD,Status.STDBY,
    # Status.PAUSE }
    # conn[Marking.PRIM] = { Conn.SEND | Conn.RECV | Conn.ACK | Conn.PAUSE }
    # dest = { Marking.PRIM, Marking.SEC }


if __name__ == "__main__":
    main()
