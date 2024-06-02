# -*- coding: utf-8 -*-
"""
    Created on Thu Mar 28 14:30:07 2024

    @author: prachi
"""

import struct
from datetime import timedelta, datetime
import sys
import os
from time import time
import xlsxwriter
from itertools import groupby
from statistics import mean
import operator


class VWAPCalculator():
    
    def __init__(self):
        """Args:
            stock_map: dictionary where key is stock_name and values is list of tuple
                       containing msg_type,hour, match_number, stock_price, share_volume
                        e.g: 'BBT-F': [('P', 14, 5796248, 24.18, 100)]
            stk_list: dictionary, where the key is the order reference number (order_ref_no) and 
                        the value is a tuple containing the stock name and price.
            exe_orders: dictionary, where key is match_number and value is tuple.
                        {match_number: (msg_type, order_ref_no, stock_name) }            
        """
        self.stk_list = {}
        self.stock_map = {}
        self.exe_orders = {}
    
    
    def get_timstamp(self,timestamp):
        """_summary_: This function takes a binary timestamp, converts it to an integer,
                    calculates the duration in seconds, extracts the hours component from the duration,
                    and returns it as an integer representing the hour. 
                    
        Args:
            timestamp (binary): _description_

        Returns:
            hour_of_the_day (int): an integer representing the hour
        """
        # Convert the binary timestamp to an integer
        int_nanoseconds = int.from_bytes(timestamp, byteorder='big')
        
        date_time='{0}'.format(timedelta(seconds= int_nanoseconds/1e9))
        
        # Extract the hour component
        hour_of_the_day= int(date_time.split(':')[0])
        return hour_of_the_day


    def add_order_message(self,message,msg_type):
        """_summary_: 
                    - 'A' indicates an "Add Order: No MPID Attribution Message,"
                    - 'F' indicates an "Add Order : MPID Attribution Message."

        Args:
            message (binary): binary message
            msg_type (_type_): Type of message which indcates about the order type.
        """
        try:
            if msg_type=='A':  
                # the structure of the binary data
                result=struct.unpack('>HH6sQsI8sI',message)
            if msg_type=='F':  
                result=struct.unpack('>HH6sQsI8sI4s',message)

            if result[4]== b'B': #if buy order
                order_ref_no = result[3]
                stock_name = result[6].strip()
                stock_price = result[7] / 10000.00 
                
                #adding order to dictionary for tracking stock name in executed orders
                self.stk_list[order_ref_no] = (stock_name, stock_price)
        except Exception as err:
            print(f"Exception while parsing add_order_message. {err}")
        return


    def broken_trade_message(self, message):
        """_summary_: This method handles broken trade messages by removing the corresponding trade
                    message from the stock_map dictionary based on the match number and other criteria.

        Args:
            message (binary): binary message
        """ 
        try:
            result=struct.unpack('>HH6sQ',message)
            match_number = result[3]
            (msg_type, order_ref_no, stock_name) = self.exe_orders.pop(match_number)
            if stock_name in self.stock_map:
                stock_list = self.stock_map[stock_name]
                for index, item in enumerate(stock_list):
                    if item[1] == order_ref_no and msg_type == item[0]:
                        del stock_list[index]
                        break
                self.stock_map[stock_name] = stock_list
        except KeyError as e:
            return	


    def cross_trade_message(self, message):
        """_summary_: This method parses a cross-trade message from a binary message, 
        extracts relevant information such as stock name, price, and volume, 
        and updates the stock_map and exe_orders dictionaries accordingly.

        Args:
            message (binary): binary message
        """
        try:
            msg_type = 'Q'
            result= struct.unpack('>HH6sQ8sIQs',message)
            stock_price=result[5]/10000.00  # converting the raw price data into dollars
            timestamp=result[2]   # hour of the day
            hour = self.get_timstamp(timestamp)
            
            share_volume = result[3]
            match_number = result[6]
            stock_name = result[4].strip()
        
            if share_volume == 0:
                return	
            elif stock_name not in self.stock_map:
                self.stock_map[stock_name] = [(msg_type,hour, match_number, stock_price, share_volume)]
            else:
                stock_list = self.stock_map[stock_name]		
                stock_list.append((msg_type,hour, match_number, stock_price, share_volume))
                self.stock_map[stock_name] = stock_list
                
            #add order to executed order map
            self.exe_orders[match_number] = (msg_type,hour, match_number, stock_name)
        except Exception as err:
            print(f"Exception while parsing cross_trade_message. Error msg: {err}")
        return

    
    
    def delete_order_message(self, message):
        """_summary_: This method handles the deletion of an order message 
        by removing the corresponding entry from the stk_list dictionary based on the order reference number.
        If the order reference number is not found in the dictionary, the method returns without further processing.

        Args:
            message (binary): binary message
        """
        result=struct.unpack('>HH6sQ',message)

        order_ref_no = result[3]
        try:
            self.stk_list.pop(order_ref_no)
        except KeyError as e:
            return	
    
    
    def replace_order_message(self, message):
        result=struct.unpack('>HH6sQQII',message)
        old_order_ref_number = result[3]
        new_order_ref_number = result[4]
        try:
            (stock_name, stock_price) = self.stk_list.pop(old_order_ref_number)
            self.stk_list[new_order_ref_number] = (stock_name, stock_price)
        except KeyError as e:
            return
        return

    def trade_message(self, message):  
        try:      
            msg_type = 'P'
            result= struct.unpack('>HH6sQsI8sIQ',message)
            
            stock_price=result[7]/10000.00
            timestamp=result[2]
            hour = self.get_timstamp(timestamp)
        
            share_volume = result[5]
            match_number = result[8]
            stock_name = result[6].strip()

            if stock_name not in self.stock_map:
                self.stock_map[stock_name] = [(msg_type,hour, match_number, stock_price, share_volume)]
            else:
                stock_list = self.stock_map[stock_name]		
                stock_list.append((msg_type,hour, match_number, stock_price, share_volume))
                self.stock_map[stock_name] = stock_list
                
            #add order to executed order map
            self.exe_orders[match_number] = (msg_type,hour, match_number, stock_name)
        except Exception as err:
            print(f"Exception while parsing trade_message. Error msg: {err}")
        return
        
        
       
    def executed_price_order_message(self, message):
        
        msg_type = 'C'
        result=struct.unpack('>HH6sQIQsI',message)
        try:
            if result[6] == b'Y':     # only considering printable
                order_ref_no = result[3]
                stock_price = (result[7]) / 10000.00 #obtain the price in dollars
                share_volume = result[4]
                match_number = result[5]
                timestamp = result[2]
                hour= self.get_timstamp(timestamp)
                
                try:
                    (stock_name, stock_price_old) = self.stk_list[order_ref_no]
                    if stock_name not in self.stock_map:
                        self.stock_map[stock_name] = [(msg_type,hour, order_ref_no, stock_price, share_volume)]
                    else:
                        stock_list = self.stock_map[stock_name]
                        stock_list.append((msg_type,hour,order_ref_no, stock_price, share_volume))
                        self.stock_map[stock_name] = stock_list
                    self.exe_orders[match_number] = (msg_type,hour, order_ref_no, stock_name)
                except KeyError as e:
                    return
        except Exception as err:
            print(f"Exception while parsing executed_price_order_message {result}. Error msg: {err}")
        return


    def executed_order_message(self, message):
        msg_type = 'E'
        result=struct.unpack('>HH6sQIQ',message)
        try:
            order_ref_no = result[3]
            stock_price = 0
            share_volume = result[4]
            match_number = result[5]
            timestamp = result[2]
            hour= self.get_timstamp(timestamp)

            try:
                (stock_name, stock_price) = self.stk_list[order_ref_no]
                if stock_name not in self.stock_map:
                    self.stock_map[stock_name] = [(msg_type,hour, order_ref_no, stock_price, share_volume)]
                else:
                    stock_list = self.stock_map[stock_name]
                    stock_list.append((msg_type,hour, order_ref_no, stock_price, share_volume))
                    self.stock_map[stock_name] = stock_list
                    #add order to executed order map
                self.exe_orders[match_number] = (msg_type,hour, order_ref_no, stock_name)
            except KeyError as e:
                return	
        except Exception as err:
            print(f"Exception while parsing executed_order_message {result}. Error msg: {err}")
        return

    def split_message(self, message, msg_type):
        if msg_type == 'P': 
            self.trade_message(message)
        elif msg_type == 'C':		
            self.executed_price_order_message(message)
        elif msg_type == 'E':		
            self.executed_order_message(message)
        elif msg_type == 'A' or msg_type == 'F':		
            self.add_order_message(message,msg_type) 
        elif msg_type == 'D':		
            self.delete_order_message(message)
        elif msg_type == 'Q':
            self.cross_trade_message(message)
        elif msg_type == 'B':		
            self.broken_trade_message(message)
        elif msg_type == 'U':		
            self.replace_order_message(message)
        else:
            return 
        
    
    
    def write_vwap_to_xcel(self, stock_vwap, output_file_name):
        workbook = xlsxwriter.Workbook(output_file_name)
        sheet = workbook.add_worksheet("VWAP Data")
        sheet.write('A1', "Stock Code")
        sheet.write('B1', "Hour")
        sheet.write('C1', "Price")
        sheet.write('D1', "Volume")
        sheet.write('E1', "Cumulative Volume")
        sheet.write('F1', "Cumulative Volume * Price")
        sheet.write('G1', "VWAP")
        row = 1
        for key, value in stock_vwap.items():
            cumulative_volume = 0
            cumulative_volume_price = 0
            for index, item in enumerate(value):
                sheet.write(row, 0, key.decode())  # Write stock code
                sheet.write(row, 1, item[0])       # Write hour
                sheet.write(row, 2, item[2])       # Write price
                sheet.write(row, 3, item[1])       # Write volume

                cumulative_volume += item[1]
                cumulative_volume_price += item[1] * item[2]

                sheet.write(row, 4, cumulative_volume)             # Write cumulative volume
                sheet.write(row, 5, cumulative_volume_price)       # Write cumulative volume * price
                sheet.write(row, 6, cumulative_volume_price / cumulative_volume if cumulative_volume else 0)  # Write VWAP

                row += 1
        workbook.close()       
            
    
    def calculate_VWAP(self):
        """_summary_: The method calculate the VWAP for each stock based on the provided trade data
                        stored in the stock_map dictionary.
                        VWAP : Volume-Weighted Average Price
                        VWAP = (Price*Volume) / Volume over a perticular period of time               
        Returns:
            stock_vwap (dictionary) : keys are stock names, and 
            the values are lists of tuples containing VWAP values calculated for different hours of trading.
        """
        stock_vwap={}        
        for stock_name, stock_meta_data in self.stock_map.items():
            x=stock_meta_data      # (msg_type,hour, match_number, stock_price, share_volume)
            
            get_sum_of_share_volums_group_by_hour = lambda tu : [(k, sum(v2[4] for v2 in v)) for k, v in groupby(tu, lambda x: x[1])]
            get_mean_of_stock_prices_group_by_hour = lambda tu : [(k, mean(v2[3] for v2 in v)) for k, v in groupby(tu, lambda x: x[1])]
            
            # hour,sum_of_share_volumes e.g: [(4, 1547), (5, 540), (6, 170), (7, 7006), (8, 260), (9, 7214)]
            aggregated_share_volume = get_sum_of_share_volums_group_by_hour(x)
            
            # hour, mean_of_share_volume e.g: [(4, 38.726875), (5, 38.684), (6, 38.785), (7, 38.89142857142857), (8, 38.38333333333333), (9, 38.60117021276596)]
            average_stock_price = get_mean_of_stock_prices_group_by_hour(x)  
            
            get_first_element = operator.itemgetter(0)
            
            # Dictionary for fast look-ups.
            average_stock_price_dict = {get_first_element(rec): rec[1:] for rec in average_stock_price} 
            
            # [(4, 1547, 38.726875), (5, 540, 38.684), (6, 170, 38.785), (7, 7006, 38.89142857142857), (8, 260, 38.38333333333333), (9, 7214, 38.60117021276596)]
            hourly_aggregated_share_volume_and_price = [sum_of_share_volume_by_hour + average_stock_price_dict[get_first_element(sum_of_share_volume_by_hour)] for sum_of_share_volume_by_hour in aggregated_share_volume if get_first_element(sum_of_share_volume_by_hour) in average_stock_price_dict]
            
            stock_vwap[stock_name]= hourly_aggregated_share_volume_and_price
        
        return stock_vwap
     
           
   
    def main(self, file_path:str, output_file_name: str) -> None:
        start = time() # just for debugging              
        
        with open(file_path,'rb') as f:
            file_size = os.path.getsize(file_path)
            file_read = 0
            
            print("File processing started successfully. it will take some time, Please keep the terminal open.")
            
            while file_read < file_size:
                try:
                    message_size = int.from_bytes(f.read(2), byteorder='big', signed=False)
                    if not message_size:
                        break
                    
                    file_read += message_size
                    message_type = f.read(1).decode('ascii')
                    record = f.read(message_size - 1)   
                                 
                    if message_type=='S':    # Start of System hours
                        result=struct.unpack('>HH6ss',record)
                        # if result[3]== b'M':    # End of Market hours
                        #     break
                        
                    # read & store message
                    self.split_message(record, message_type)
                except Exception as e:
                    pass
        
        print("Time taken to parse the stock data ", timedelta(seconds=time() - start))
        
        # Freeing up the memory
        self.exe_orders = {}
        self.stk_list = {}
        
        stock_vwap = self.calculate_VWAP()
        self.write_vwap_to_xcel(stock_vwap, output_file_name)
        print("Time taken to parse and calculate the VWAP", timedelta(seconds=time() - start))
        return
        



if __name__ == "__main__":
    
    input_file = input("Please provide the path of itch unzipped file: ")
    if not input_file or not os.path.exists(input_file):
        print("Please provide a valid file path. Exiting.")
        sys.exit()
    
    output_path = input("Please provide the output path: ")
    if not output_path:
        output_path = os.getcwd()
    output_file = output_path + "/vwap.xlsx"
        
    VWAPCalculator().main(input_file, output_file)
    
    print(f"Processiong Completed. Please check the output in {output_file}")
