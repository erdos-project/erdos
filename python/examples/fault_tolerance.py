import erdos
import time
from erdos.operators import map
from utils import Marking, Status, Conn, send_data
from buffer import Buffer


class DestOp(erdos.Operator):
    """ 1 in 1 out operator

        In1 - Receives data + watermark from EgressOp
        Out1 - Sends watermark to EgressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_watermark_stream: erdos.WriteStream):
        self.buffer = Buffer()
        read_stream.add_callback(self.callback)
        read_stream.add_watermark_callback(self.watermark_callback,
                                           [write_watermark_stream])

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

    def callback(self, msg):
        print("{name}: received {msg}".format(msg=msg, name=self.config.name))
        self.buffer.put(msg, msg.timestamp, {Marking.PROD})

    def watermark_callback(self, timestamp, write_watermark_stream):
        watermark = erdos.WatermarkMessage(timestamp)
        print("{name}: sending watermark {watermark} to EgressOp".format(
            watermark=watermark, name=self.config.name))
        write_watermark_stream.send(watermark)


class SourceOp(erdos.Operator):
    """ 1 out operator

        Out1 - Sends data to IngressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream):
        self.buffer = Buffer()
        self.write_stream = write_stream
        read_stream.add_watermark_callback(self.ack_watermark_callback)

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]

    def ack_watermark_callback(self, timestamp):
        print("{name}: clearing SourceOp buffer messages of timestamp <= {ts}".
              format(name=self.config.name, ts=timestamp))

    def run(self):
        count = 0
        while True:
            timestamp = erdos.Timestamp(coordinates=[count])
            payload = [
                erdos.Message(timestamp, count),
                erdos.Message(timestamp, count + 1),
                erdos.Message(timestamp, count + 2)
            ]
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
        Out1 - Sends data + watermark to SConsPOp and SConsSOp (write_stream)
        Out2 - Sends watermark to SourceOp (watermark_write_stream)
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 primary_cons_watermark_stream: erdos.ReadStream,
                 secondary_cons_watermark_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream,
                 watermark_write_stream: erdos.WriteStream):
        self.buffer = Buffer()
        self.conn = dict.fromkeys([Marking.PRIM, Marking.SEC],
                                  {Conn.SEND, Conn.ACK})
        self.delete = {Marking.PROD, Marking.PRIM, Marking.SEC}
        self.dest = Marking.PRIM
        self.status = Status.ACTIVE

        # Gets watermarks from SourceOp
        read_stream.add_watermark_callback(self.src_watermark_callback,
                                           [write_stream])
        # Gets data messages from SourceOp
        read_stream.add_callback(self.callback, [watermark_write_stream])
        erdos.add_watermark_callback(
            [primary_cons_watermark_stream, secondary_cons_watermark_stream],
            [], self.ack_watermark_callback)

    @staticmethod
    def connect(read_stream, primary_cons_watermark_stream,
                secondary_cons_watermark_stream):
        write_stream = erdos.WriteStream()
        watermark_write_stream = erdos.WriteStream()
        return [write_stream, watermark_write_stream]

    def callback(self, msg, watermark_write_stream):
        print("{name}: received message {msg}".format(msg=msg,
                                                      name=self.config.name))
        # This message was produced from below, so its marking is set to Marking.PROD
        self.buffer.put(msg, msg.timestamp, {Marking.PROD})
        watermark = erdos.WatermarkMessage(msg.timestamp)
        print("{name}: sending watermark {watermark} to SourceOp".format(
            watermark=watermark, name=self.config.name))
        watermark_write_stream.send(watermark)

    def src_watermark_callback(self, timestamp: erdos.Timestamp,
                               write_stream: erdos.WriteStream):
        # After getting watermark from SourceOp, sends data to consumers
        print(
            "{name}: received watermark at timestamp {timestamp} from SourceOp"
            .format(timestamp=timestamp, name=self.config.name))

        data_tuples = self.buffer.watermark(timestamp)
        send_data(data_tuples,
                  write_downstream=write_stream,
                  sender=self.config.name)

    def ack_watermark_callback(self, timestamp):
        print("{name}: received primary & secondary watermarks at {timestamp}".
              format(timestamp=timestamp, name=self.config.name))
        # Clears buffer of messages <= timestamp
        print("QUEUE BEFORE", self.buffer)
        # self.buffer.add_dest_ack(timestamp, {Marking.PROD, Marking.SEC})
        self.buffer.watermark(timestamp, clear=True)
        # self.buffer.ack(timestamp, Marking.PRIM, self.delete)
        # self.buffer.ack(timestamp, Marking.SEC, self.delete)
        print("QUEUE AFTER", self.buffer)


class SConsOp(erdos.Operator):
    """ 1 in 2 out operator

        In1 - Receives data + watermark from IngressOp
        Out1 - Sends data + watermark to SProdPOp
        Out2 - Sends watermark to IngressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream,
                 write_watermark_stream: erdos.WriteStream):
        self.buffer = Buffer()
        self.status = Status.ACTIVE
        self.conn = {}
        if self.config.name == "SConsPOp":
            self.conn[Marking.PRIM] = Conn.SEND
        elif self.config.name == "SConsSOp":
            self.conn[Marking.SEC] = Conn.SEND
        self.dest = Marking.PRIM
        self.delete = {Marking.PROD}
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
        self.buffer.put(msg, msg.timestamp, {Marking.PROD})

    def watermark_callback(self, timestamp, write_stream,
                           write_watermark_stream):
        print("{name}: received watermark from IngressOp".format(
            name=self.config.name))
        print(self.config.name + " QUEUE BEFORE " + str(timestamp),
              self.buffer)
        data_tuples = self.buffer.watermark(timestamp, clear=True)
        send_data(data_tuples, write_stream, write_watermark_stream,
                  self.config.name)
        print(self.config.name + " QUEUE AFTER " + str(timestamp), self.buffer)


class SProdPOp(erdos.Operator):
    """ 1 in 1 out operator

        In1 - Receives data + watermark from SConsPOp (primary consumer op)
        Out1 - Sends data + watermark to EgressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 write_stream: erdos.WriteStream):
        self.buffer = Buffer()
        self.conn = {}
        self.conn[Marking.PRIM] = Conn.SEND
        self.delete = {Marking.PROD}
        self.dest = Marking.PRIM
        self.status = Status.ACTIVE
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
        self.buffer.put(msg, msg.timestamp, {Marking.PROD})

    def watermark_callback(self, timestamp, write_stream):
        print("{name}: received watermark at timestamp {ts} from SConsPOp".
              format(ts=timestamp, name=self.config.name))
        print(self.config.name + " QUEUE BEFORE", self.buffer)
        data_tuples = self.buffer.watermark(timestamp, clear=True)
        send_data(data_tuples, write_stream, sender=self.config.name)
        print(self.config.name + " QUEUE AFTER", self.buffer)


class SProdSOp(erdos.Operator):
    """ 2 in operator

        In1 - Receives data + watermark from SConsSOp (Secondary consumer op)
        In2 - Receives watermark from EgressOp
    """
    def __init__(self, read_stream: erdos.ReadStream,
                 egress_watermark_stream: erdos.ReadStream):
        self.buffer = Buffer()
        self.conn = {}
        self.conn[Marking.PRIM] = Conn.ACK  # data is not forwarded
        self.delete = {Marking.PRIM, Marking.SEC, Marking.PROD}
        self.dest = Marking.PRIM
        self.status = Status.ACTIVE
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
        self.buffer.put(msg, msg.timestamp, {Marking.PROD})

    def watermark_callback(self, timestamp):
        print("{name}: received watermark at {ts} from SConsSOp".format(
            name=self.config.name, ts=timestamp))

    def egress_watermark_callback(self, timestamp):
        # Clear the buffer with timestamps <= timestamp
        print("{name}: received watermark from EgressOp".format(
            name=self.config.name))
        print(self.config.name + " QUEUE BEFORE", self.buffer)
        data_tuples = self.buffer.watermark(timestamp, clear=True)
        send_data(data_tuples, sender=self.config.name)
        print(self.config.name + " QUEUE AFTER", self.buffer)


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
        self.buffer = Buffer()
        self.conn = {}
        self.status = Status.ACTIVE
        read_stream.add_callback(self.callback)
        read_stream.add_watermark_callback(
            self.watermark_callback, [write_stream, write_watermark_stream])
        dest_watermark_stream.add_watermark_callback(
            self.ack_watermark_callback)
        # self.delete = {Marking.PROD, Marking.}

    @staticmethod
    def connect(read_stream, dest_watermark_stream):
        write_stream = erdos.WriteStream()
        write_watermark_stream = erdos.WriteStream()
        return [write_watermark_stream, write_stream]

    def callback(self, msg):
        print("{name}: received {msg} from SProdPOp".format(
            msg=msg, name=self.config.name))
        self.buffer.put(msg, msg.timestamp, {Marking.PROD})

    def watermark_callback(self, timestamp, write_stream,
                           write_watermark_stream):
        print(
            "{name}: received watermark at timestamp {timestamp} from SProdPOp"
            .format(timestamp=timestamp, name=self.config.name))
        print("EGRESS OP: BEFORE BUFFER", self.buffer)
        data_tuples = self.buffer.watermark(timestamp)
        for msg, marking in data_tuples:
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

    def ack_watermark_callback(self, timestamp):
        # Clear the buffer with timestamps <= timestamp
        print(
            "{name}: received watermark at timestamp {timestamp} from DestOp".
            format(timestamp=timestamp, name=self.config.name))
        print("EGRESS OP: BEFORE BUFFER", self.buffer)
        self.buffer.watermark(timestamp, clear=True)
        print("EGRESS OP: AFTER BUFFER", self.buffer)


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

    watermark_src_stream = erdos.LoopStream()

    # Data comes out of SourceOp
    (source_stream, ) = erdos.connect(
        SourceOp, erdos.OperatorConfig(name="SourceOp", flow_watermarks=False),
        [watermark_src_stream])

    primary_cons_watermark_stream = erdos.LoopStream()
    secondary_cons_watermark_stream = erdos.LoopStream()

    # Data flows into IngressOp and out of IngressOp again
    (ingress_stream, watermark_stream) = erdos.connect(
        IngressOp, erdos.OperatorConfig(name="IngressOp",
                                        flow_watermarks=False),
        [
            source_stream, primary_cons_watermark_stream,
            secondary_cons_watermark_stream
        ])

    watermark_src_stream.set(watermark_stream)

    # Data comes into primary SConsOp and out of primary SConsOp
    (primary_cons_stream, primary_watermark_stream) = erdos.connect(
        SConsOp, erdos.OperatorConfig(name="SConsPOp", flow_watermarks=False),
        [ingress_stream])

    primary_cons_watermark_stream.set(primary_watermark_stream)

    # Data comes into secondary SConsOp and out of secondary SConsOp
    (secondary_cons_stream, secondary_watermark_stream) = erdos.connect(
        SConsOp, erdos.OperatorConfig(name="SConsSOp", flow_watermarks=False),
        [ingress_stream])

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
        SProdPOp, erdos.OperatorConfig(name="SProdPOp", flow_watermarks=False),
        [primary_map_stream])

    # Data from Ingress passed into EgressOp and EgressOp
    # returns WatermarkMessage
    dest_watermark_stream = erdos.LoopStream()
    (egress_watermark_stream, egress_dest_stream) = erdos.connect(
        EgressOp, erdos.OperatorConfig(name="EgressOp", flow_watermarks=False),
        [primary_prod_stream, dest_watermark_stream])

    # EgressOp sends Watermark to SProdSOp to clear its buffer of messages.
    # Also SProdSOp receives data from secondary intermediate operators
    erdos.connect(SProdSOp,
                  erdos.OperatorConfig(name="SProdSOp", flow_watermarks=False),
                  [secondary_map_stream, egress_watermark_stream])

    # Data from EgressOp flows into DestOp and DestOp
    # sends WatermarkMessage back to EgressOp to clear its buffer
    (watermark_stream, ) = erdos.connect(
        DestOp, erdos.OperatorConfig(name="DestOp", flow_watermarks=False),
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
