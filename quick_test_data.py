"""Quick Test Data Generator - No Emojis Version"""

import sqlite3
from datetime import datetime, timedelta
import random

def generate_test_snapshots_direct(days=365):
    """Generate dummy snapshots directly to database"""
    
    conn = sqlite3.connect('stock_management.db')
    cursor = conn.cursor()
    
    print(f"Generating {days} days of test snapshots...")
    
    base_quantity = 1000
    base_value = 50000.0
    
    today = datetime.now().date()
    created_count = 0
    
    for i in range(days, -1, -1):
        snapshot_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        
        trend_factor = 1 + (days - i) * 0.001
        random_variation = random.uniform(0.85, 1.15)
        quantity = int(base_quantity * trend_factor * random_variation)
        
        value_variation = random.uniform(0.90, 1.10)
        value = round(base_value * trend_factor * value_variation, 2)
        
        month = (today - timedelta(days=i)).month
        if month in [11, 12]:
            quantity = int(quantity * 1.2)
            value = value * 1.2
        elif month in [1, 2]:
            quantity = int(quantity * 0.8)
            value = value * 0.8
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO stock_snapshots 
                (snapshot_date, total_quantity, total_value)
                VALUES (?, ?, ?)
            ''', (snapshot_date, quantity, value))
            created_count += 1
            
            if (days - i) % 30 == 0:
                print(f"  Created: {snapshot_date} - Qty={quantity}, Value={value:.2f}")
        
        except Exception as e:
            print(f"  Error for {snapshot_date}: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"\nSUCCESS: Created {created_count} snapshots!")
    print(f"Date range: {(today - timedelta(days=days)).strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
    print("\nNow you can test all duration buttons:")
    print("  7D, 30D, 60D, 90D, 180D, 360D, All")
    print("\nRefresh your dashboard and try clicking the buttons!")

if __name__ == "__main__":
    print("=" * 60)
    print("  TEST DATA GENERATOR")
    print("=" * 60)
    print()
    
    generate_test_snapshots_direct(365)
