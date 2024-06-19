

import time
import struct


order_tracker = {}
# value = (ticker,price,qty)
#tracks ref number of a order
trade_tracker = {}
#value = (num,den,match_number), key = ticker 
#traders the running vwap of a ticker 

def handle_add_order_msg_A(message):
    '''
    order message type: A
    this function handles the senario a order is added to the order book, the order dictionary is updated
    '''
    fields=struct.unpack('>HH6sQsI8sI',message[1:])
    order_ref = fields[3]
    ticker = fields[6].decode().strip()
    price = fields[7] / 10000
    qty = fields[5]
    # A(no MPID attribution)
    order_tracker[order_ref] = (ticker,price,qty)

def handle_add_order_msg_F(message):
    '''
    order message type: F
    this function handles the senario a order is added to the order book, the order dictionary is updated
    '''
    f_order_ref = int.from_bytes(message[11:19], byteorder='big')
    f_qty = int.from_bytes(message[20:24], byteorder='big')
    f_ticker = str(message[24:32],'ascii').strip()
    f_price = int.from_bytes(message[32:36], byteorder='big') /10000
    # F(MPID attribution)
    #we dont care about buy or sell right??
    order_tracker[f_order_ref] = (f_ticker,f_price,f_qty)
    # print(order_tracker)

def handle_executed_order_msg(message):
    '''
    order message type : E
    this function handles the senario where the whole order or part of the order is executed. 
    if only part of the order is executed, then the ref num remains but the qty is adjusted
    '''
    e_fields=struct.unpack('>HH6sQIQ',message[1:])
    order_ref= e_fields[3]
    sold_qty = e_fields[4]
    match_number= e_fields[5]
    (ticker,price,quantity) = order_tracker[order_ref]
    # print("t: ",t)
    if quantity - sold_qty == 0:
        del order_tracker[order_ref]
    else:
        order_tracker[order_ref] = (ticker,price,quantity - sold_qty)
    on_trade(ticker,sold_qty,price,match_number)

def handle_executed_order_diff_price_msg(message):
    '''
    order message type: C
    '''
    #parsing message
    global order_tracker
    c_fields = struct.unpack('>HH6sQIQsI',message[1:])
    if c_fields[6].decode() == "Y":
        order_ref = c_fields[3]
        qty = c_fields[4]
        match_num = c_fields[5]
        new_price = c_fields[7] /10000
    #modifying order dictionary 
        (ticker,price,quantity) = order_tracker[order_ref]
        if (quantity - qty) == 0:
            del order_tracker[order_ref]
        else:
            order_tracker[order_ref] = (ticker,price,quantity - qty)
        on_trade(ticker,qty,new_price,match_num)

def handle_partial_cancellation(message):
    '''
    order message type: X
    '''
    x_fields = struct.unpack('>HH6sQI',message[1:])
    order_ref = x_fields[3]
    cancelled_shares = x_fields[4]
    try:
        (t,p,q) = order_tracker[order_ref] #type is a tuple
        if q == cancelled_shares:
            del order_tracker[order_ref]
        else:
            order_tracker[order_ref] = (t,p,q - cancelled_shares)
    except KeyError:
        print(f"{order_ref} not found")

def handle_full_order_cancellation(order_ref):
    '''
    order message type: D
    remove order from dictionary
    '''
    order_tracker.pop(order_ref)

def replace_order_msg(message):
    '''
    order message type: U
    replaces old order with new order number 
    '''
    #parsing message
    old_order_ref = int.from_bytes(message[11:19], byteorder='big')
    new_order_ref = int.from_bytes(message[19:27], byteorder='big')
    qty = int.from_bytes(message[27:31], byteorder='big')
    price = int.from_bytes(message[31:35], byteorder='big') /10000
    #modifying order dictionary
    tup = order_tracker.pop(old_order_ref)
    # tup = (ticker,price,qty)
    order_tracker[new_order_ref] = (tup[0],price,qty)

def handle_broken_messages(message):
    '''
    order message type: B
    remove broken messages 
    '''
    match_number = int.from_bytes(message[11:19], byteorder='big')
    global trade_tracker
    trade_tracker = {k: v for k, v in trade_tracker.items() if v[2] != match_number }

def handle_non_cross_trade_messages(message):
    '''
    order message type: P
    handles non displayable messages 
    '''
    p_ticker = str(message[24:32],'ascii').strip()
    p_price = int.from_bytes(message[32:36], byteorder='big') /10000
    p_qty = int.from_bytes(message[32:36], byteorder='big')
    p_match_num = int.from_bytes(message[36:42], byteorder='big')
    on_trade(p_ticker,p_qty,p_price,p_match_num)

def on_trade(ticker,qty,price,match_number):
    if ticker not in trade_tracker:
        trade_tracker[ticker] = (0,0)
    trade_tracker[ticker] = ((trade_tracker[ticker][0]+ (qty *price)),(trade_tracker[ticker][1]+qty),match_number)

def convert_to_hours(nanoseconds):
    return  nanoseconds / (((10**9) * 60) * 60)

def f_generator(data):        
    for k,v in data.items():
        (num,den,match_num) = v
        vwap = round(num / den,2)
        yield f"{k}:{vwap}\n"


def calculate_vwap(hour):
        file_name = f"../out/{hour}.txt"
        contents = f_generator(trade_tracker)
        with open(file_name,"w") as file:
            for line in contents:
                file.write(line) 
            file.close()


def main(f_name):
    start_time = time.time()
    input = open(f_name,'rb')
    with open(f_name,'rb') as input:
        size_of_msg = int.from_bytes(input.read(2), "big")
        flag =  0
        hour = 0 
        while size_of_msg:
                message = input.read(size_of_msg)
                type = chr(message[0])
                timestamp = int.from_bytes(message[5:11], byteorder='big')
                timestamp_formatted = convert_to_hours(timestamp)
                if hour != int(timestamp_formatted):
                    flag = 0
                if flag == 0:
                    calculate_vwap(hour)
                    hour = hour + 1
                    flag = 1
                if type == "A":
                    handle_add_order_msg_A(message)
                elif type == "F":
                    handle_add_order_msg_F(message)
                elif type == "E":
                    handle_executed_order_msg(message)
                elif type == "C":
                        handle_executed_order_diff_price_msg(message)
                elif type == "X":
                    handle_partial_cancellation(message)
                elif type == "D":
                    handle_full_order_cancellation(int.from_bytes(message[11:19], byteorder='big'))
                elif type == "U":
                    replace_order_msg(message)
                # elif type == "P":
                #     handle_non_displayable_orders(message)
                elif type == "B":
                    handle_broken_messages(message)
                elif type == "S":
                    s_type = chr(message[11])
                    if s_type == "M":
                        print("market_close")
                        calculate_vwap(int(timestamp_formatted))
                        print("--- %s seconds ---" % (time.time() - start_time))
                        break
                size_of_msg = int.from_bytes(input.read(2),'big')





main("../data/01302019.NASDAQ_ITCH50")



