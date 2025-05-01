import argparse
import socket
import sys
from collections import defaultdict

from utils import PacketHeader, compute_checksum

START_TYPE = 0
END_TYPE   = 1
DATA_TYPE  = 2
ACK_TYPE   = 3

HEADER_SIZE = 16

def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    connection_active = False  
    expected_seq = 0
    data_buffer = defaultdict(bytes)  
    end_received = False
    end_seq = None

    while True:
        pkt, addr = s.recvfrom(2048)
        header = PacketHeader(pkt[:HEADER_SIZE])
        payload = pkt[HEADER_SIZE:HEADER_SIZE+header.length]

        old_chksum = header.checksum
        header.checksum = 0
        computed = compute_checksum(bytes(header) + payload)
        if computed != old_chksum:
            continue

        if header.type == START_TYPE:
            if not connection_active:
                if header.seq_num == 0:
                    connection_active = True
                    expected_seq = 1
                    ack_pkt = create_ack(expected_seq)
                    s.sendto(ack_pkt, addr)

        elif header.type == DATA_TYPE:
            if connection_active:
                seq_num = header.seq_num
                if seq_num >= expected_seq and seq_num < expected_seq + window_size:
                    data_buffer[seq_num] = payload

                    while data_buffer.get(expected_seq, None) is not None:
                        expected_seq += 1

                    ack_pkt = create_ack(expected_seq)
                    s.sendto(ack_pkt, addr)
                else:
 
                    ack_pkt = create_ack(expected_seq)
                    s.sendto(ack_pkt, addr)

        elif header.type == END_TYPE:
            if connection_active:

                end_seq = header.seq_num
                ack_pkt = create_ack(end_seq + 1)
                s.sendto(ack_pkt, addr)

                full_data = b""
                for i in range(1, end_seq):
                    if i in data_buffer:
                        full_data += data_buffer[i]
                sys.stdout.buffer.write(full_data)
                sys.stdout.buffer.flush()

                break

        else:

            pass

    s.close()

def create_ack(seq_num):

    from utils import PacketHeader, compute_checksum
    header = PacketHeader(type=ACK_TYPE, seq_num=seq_num, length=0, checksum=0)
    raw = bytes(header)
    chksum = compute_checksum(raw)
    header.checksum = chksum
    return bytes(header)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="The IP address of the host that receiver is running on")
    parser.add_argument("receiver_port", type=int, help="The port number on which receiver is listening")
    parser.add_argument("window_size", type=int, help="Maximum number of outstanding packets")
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)

if __name__ == "__main__":
    main()
