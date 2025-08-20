from fastapi import FastAPI, HTTPException
import pandas as pd
import numpy as np
import re # To extract numbers from descriptions
import uuid
from pydantic import BaseModel
import requests
import os

# === FastAPI Setup ===
app = FastAPI()

class LedgerSettlement:
    def __init__(self, days): 
        """
        Initialize the LedgerSettlement class.
        Args:
            days (int): Number of days for allowable date difference in matching.
        """
        self.transdays = days
        self.results = []

    def load_ledger(self, file):
        """
        Load and clean the ledger data from a CSV file.
        - Reads the ledger file.
        - Cleans and fills missing values for key columns.
        - Extracts numbers from the Description field.
        - Adds a 'Matched' column to track matched transactions.
        """
        ledger = pd.read_csv(file) 

        # Clean and prepare data
        ledger['Journal number'] = ledger['Journal number'].fillna("")
        ledger['Voucher'] = ledger['Voucher'].fillna("")
        ledger['Amount'] = pd.to_numeric(ledger['Amount'], errors='coerce').fillna(0)
        ledger['Date'] = pd.to_datetime(ledger['Date'], errors='coerce')  # Ensure Date is in datetime format
        ledger['Description'] = ledger['Description'].fillna("")
        ledger['CostCentre'] = ledger['CostCentre'].fillna("")
        ledger['ProfitCentre'] = ledger['ProfitCentre'].fillna("")

        # Determine Type based on Amount
        ledger['Type'] = ledger['Amount'].apply(lambda x: 'debit' if x < 0 else 'credit')

        # Extract numbers from description
        ledger['ExtractedNumbers'] = ledger['Description'].apply(lambda x: re.findall(r'\d+', str(x)))
        ledger['Matched'] = False  # Initialize a column to track matched transactions
        ledger['Read'] = False  # Initialize a column to track read status
    
        self.ledger = ledger

    def settle_ledger(self):
        """
        For each account, attempts to match debits and credits within the allowed date range and matching cost/profit centres.
        Calls match_credits for each debit to find the best matching credit.
        """
        for account, group in self.ledger.groupby('MainAccount'):
            filtered = self.ledger[
                (self.ledger['Type'] == 'debit') &
                (self.ledger['MainAccount'] == account) &
                (self.ledger['Read'] == False)
            ].sort_values(by='Date')

            while not filtered.empty:
                debit = filtered.iloc[0]
                self.ledger.at[debit.name, 'Read'] = True
                #debit_numbers = set(debit['ExtractedNumbers'])
                #best_overlap = 0
                #best_credit_idx = None
                unique_guid = str(uuid.uuid4())
                self.append_debit = False

                costcentre = debit['CostCentre']
                profitcentre = debit['ProfitCentre']

                unsettletrans = self.ledger[(self.ledger['Read'] == False)
                                & (self.ledger['Matched'] == False)
                                & (self.ledger['CostCentre'] == costcentre) 
                                & (self.ledger['ProfitCentre'] == profitcentre)
                                & (self.ledger['Date'] >= debit['Date'] - pd.Timedelta(days=self.transdays))
                                & (self.ledger['Date'] <= debit['Date'] + pd.Timedelta(days=self.transdays))].sort_values(by='Date')
                
                self.match_credits_number(account, costcentre, profitcentre, debit, unsettletrans, unique_guid)
                
                filtered = self.ledger[
                    (self.ledger['Type'] == 'debit') &
                    (self.ledger['MainAccount'] == account) &
                    (self.ledger['Read'] == False)
                ].sort_values(by='Date')

    def match_credits_number(self, account, costcentre, profitcentre, debit_row, credits, unique_guid):
        """
        For a given debit, finds the best matching credit based on number overlap and allowed date difference.
        Marks matched credits and appends results to the results list.
        Args:
            account: Account identifier
            costcentre: Cost centre value
            profitcentre: Profit centre value
            debit_row: The debit row (Series)
            credits: DataFrame of candidate credits
            unique_guid: Unique settlement identifier
        """
        debit_numbers = set(debit_row['ExtractedNumbers'])
        #best_overlap = 0
        best_credit_idx = None

        for j, credit_row in credits.iterrows():
            if(credit_row['Matched'] == True):
                print("Credit already matched")
                continue

            date_diff = (credit_row['Date'] - debit_row['Date']).days
            if date_diff < -self.transdays or date_diff > self.transdays:
                continue

            best_overlap = 0
            credit_numbers = set(credit_row['ExtractedNumbers'])

            common = [n1 for n1 in debit_numbers for n2 in credit_numbers if n1 in n2 or n2 in n1]
            overlap = len(common)
            
            if overlap > best_overlap:
                best_overlap = overlap
                best_credit_idx = j

            if best_credit_idx is not None and best_overlap > 0:
                credit_row = credits.loc[best_credit_idx]
                self.append_debit = True
                self.ledger.at[credit_row.name, 'Matched'] = True  # Mark this credit as matched
                self.results.append({
                    'Journal number': credit_row['Journal number'],
                    'Voucher': credit_row['Voucher'],
                    'Date': credit_row['Date'],
                    'Account': account,
                    'CostCentre': costcentre,
                    'ProfitCentre': profitcentre,
                    'Description': credit_row['Description'],
                    'Amount': credit_row['Amount'],
                    'Settlement_Number': unique_guid
                    #,'Extracted_Numbers': credit_row['ExtractedNumbers']
                })

        if(self.append_debit):
            self.ledger.at[debit_row.name, 'Matched'] = True  # Mark this debit as matched
            self.results.append({
                    'Journal number': debit_row['Journal number'],
                    'Voucher': debit_row['Voucher'],
                    'Date': debit_row['Date'],  
                    'Account': account,
                    'CostCentre': costcentre,
                    'ProfitCentre': profitcentre,
                    'Description': debit_row['Description'],
                    'Amount': debit_row['Amount'],
                    'Settlement_Number': unique_guid
                    #'Extracted_Numbers': debit_row['ExtractedNumbers']
            })


    def write_unsettled(self):
        """
        Appends all unmatched (unsettled) transactions to the results list for output.
        """
        remaining = self.ledger[self.ledger['Matched'] == False]

        for i, remaining_row in remaining.iterrows():
            self.results.append({
                'Journal number': remaining_row['Journal number'],
                'Voucher': remaining_row['Voucher'],
                'Date': remaining_row['Date'],  
                'Account': remaining_row['MainAccount'],
                'CostCentre': remaining_row['CostCentre'],
                'ProfitCentre': remaining_row['ProfitCentre'],
                'Description': remaining_row['Description'],
                'Amount': remaining_row['Amount'],
                'Settlement_Number': ''
                #'Extracted_Numbers': remaining_row['ExtractedNumbers']
                })
            
    def write_results(self, file):
        """
        Writes the results list to a CSV file named 'matched_transactions.csv'.
        """
        matched_df = pd.DataFrame(self.results)
        matched_df.to_csv(file, index=False)
        print('Matching complete. Results saved to matched_transactions.csv')

# === MCP Endpoint ===
@app.post("/api/mcp")
def process_mcp():
    settle = LedgerSettlement(3)
    settle.load_ledger('Ledger settlements_638851462197217174.csv')
    settle.settle_ledger()
    settle.write_unsettled()
    settle.write_results('matched_transactions.csv')
    # You need to define McpResponse and results, or use a standard response
    return {"message": "Matching complete. Results saved to matched_transactions.csv"}