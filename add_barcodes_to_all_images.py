# -*- coding: utf-8 -*-
"""
Add real barcode images to ALL product variant images
Final version - processes all products
"""

import os
import sys
from PIL import Image
import requests
from io import BytesIO
from database import StockDatabase
from barcode import EAN13
from barcode.writer import ImageWriter

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def create_barcode_image(barcode_number):
    """
    Create barcode image in memory
    
    Args:
        barcode_number: 13-digit EAN-13 barcode
    
    Returns:
        PIL Image object
    """
    # Create barcode writer
    writer = ImageWriter()
    writer.set_options({
        'module_width': 0.4,
        'module_height': 15.0,
        'quiet_zone': 6.5,
        'text_distance': 5.0,
        'font_size': 14,
        'write_text': True,
    })
    
    # Generate barcode
    ean = EAN13(barcode_number, writer=writer)
    
    # Save to BytesIO
    buffer = BytesIO()
    ean.write(buffer)
    buffer.seek(0)
    
    # Load as PIL Image
    barcode_img = Image.open(buffer)
    
    return barcode_img

def add_barcode_below_image(product_image, barcode_number, output_path):
    """
    Add barcode image below the product image (wider and shorter using CROP technique)
    
    Args:
        product_image: PIL Image object of product
        barcode_number: Barcode number
        output_path: Path to save the new image
    """
    # Get product image dimensions
    img_width, img_height = product_image.size
    
    # Create barcode image
    barcode_img = create_barcode_image(barcode_number)
    
    # STEP 1: Resize width to match product width (95%)
    target_width = int(img_width * 0.95)  # 95% of product width
    
    # Calculate new height maintaining aspect ratio
    aspect_ratio = barcode_img.height / barcode_img.width
    resized_height = int(target_width * aspect_ratio)
    
    # Resize to target width
    barcode_resized = barcode_img.resize((target_width, resized_height), Image.Resampling.LANCZOS)
    
    # STEP 2: CROP from top to reduce bar height (keep bottom 50% which includes numbers)
    # This crops the tall bars from the top while keeping the numbers at the bottom
    crop_percentage = 0.5  # Keep bottom 50% (adjust this to control final height)
    crop_start_y = int(resized_height * (1 - crop_percentage))  # Start cropping from here
    
    # Crop: (left, top, right, bottom)
    barcode_cropped = barcode_resized.crop((0, crop_start_y, target_width, resized_height))
    
    # STEP 3: CROP bottom padding (remove white space below numbers)
    # The barcode library adds extra padding at the bottom, let's remove it
    bottom_crop_percentage = 0.70  # Keep only 70% (remove 30% from bottom)
    crop_bottom_y = int(barcode_cropped.height * bottom_crop_percentage)
    
    # Crop to remove bottom padding
    barcode_cropped_bottom = barcode_cropped.crop((0, 0, barcode_cropped.width, crop_bottom_y))
    
    # STEP 4: RESIZE final barcode to 50%
    final_resize_percentage = 0.5  # 50% of current size
    final_width = int(barcode_cropped_bottom.width * final_resize_percentage)
    final_height = int(barcode_cropped_bottom.height * final_resize_percentage)
    
    barcode_final = barcode_cropped_bottom.resize((final_width, final_height), Image.Resampling.LANCZOS)
    
    final_barcode_width = barcode_final.width
    final_barcode_height = barcode_final.height
    
    # Calculate new image height with minimal padding
    padding = 5  # Minimal padding
    new_height = img_height + final_barcode_height + padding * 2
    
    # Create new image with white background
    new_img = Image.new('RGB', (img_width, new_height), 'white')
    
    # Paste product image at the top
    new_img.paste(product_image, (0, 0))
    
    # Paste barcode image CENTERED below product
    barcode_x = (img_width - final_barcode_width) // 2  # Centered
    barcode_y = img_height + padding
    new_img.paste(barcode_final, (barcode_x, barcode_y))
    
    # Save the new image
    new_img.save(output_path, quality=95)

def process_all_products(output_dir='Product_Images_With_Barcodes'):
    """
    Process ALL products and add barcodes to their images
    
    Args:
        output_dir: Output directory for images with barcodes
    """
    print("Starting barcode addition for ALL products...")
    print("=" * 60)
    
    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    # Connect to database
    print("Connecting to database...")
    db = StockDatabase()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Get all variants with barcodes and images
    print("Fetching all variants with images...")
    cursor.execute('''
        SELECT 
            bp.id as product_id,
            bp.product_code,
            c.color_name,
            ci.image_url,
            b.barcode_number
        FROM product_variants pv
        JOIN base_products bp ON pv.base_product_id = bp.id
        JOIN colors c ON pv.color_id = c.id
        LEFT JOIN color_images ci ON pv.id = ci.variant_id
        LEFT JOIN barcodes b ON pv.id = b.variant_id
        WHERE ci.image_url IS NOT NULL 
        AND ci.image_url != ''
        AND b.barcode_number IS NOT NULL
        ORDER BY bp.product_code, c.color_name
    ''')
    
    variants = cursor.fetchall()
    conn.close()
    
    if not variants:
        print("No variants with images and barcodes found!")
        return
    
    total_to_process = len(variants)
    print(f"Found {total_to_process} images to process")
    print("=" * 60)
    
    successful = 0
    failed = 0
    current_product_code = None
    product_folder = None
    
    # Process each variant
    for idx, variant in enumerate(variants, 1):
        product_id = variant[0]
        product_code = variant[1] or 'Unknown'
        color_name = variant[2] or 'Unknown'
        image_url = variant[3]
        barcode_number = variant[4]
        
        # Create new product folder if needed
        if product_code != current_product_code:
            current_product_code = product_code
            product_folder = os.path.join(output_dir, product_code)
            if not os.path.exists(product_folder):
                os.makedirs(product_folder)
            print(f"\n[{product_code}]")
        
        try:
            # Download image
            response = requests.get(image_url, timeout=10)
            response.raise_for_status()
            
            # Open image from memory
            img = Image.open(BytesIO(response.content))
            
            # Convert to RGB if necessary
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Create output filename: color_barcode.jpg
            output_filename = f"{color_name}_{barcode_number}.jpg"
            output_path = os.path.join(product_folder, output_filename)
            
            # Add barcode below image
            add_barcode_below_image(img, barcode_number, output_path)
            
            print(f"  [{idx}/{total_to_process}] OK: {color_name}")
            successful += 1
            
        except Exception as e:
            print(f"  [{idx}/{total_to_process}] ERROR: {color_name} - {str(e)[:40]}")
            failed += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE!")
    print(f"  Total images: {total_to_process}")
    print(f"  Successfully processed: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Success rate: {(successful/total_to_process*100):.1f}%")
    print(f"  Output folder: {os.path.abspath(output_dir)}")
    print("=" * 60)

if __name__ == "__main__":
    print("=" * 60)
    print("PRODUCT IMAGES WITH BARCODES - FULL PROCESSING")
    print("=" * 60)
    
    # Process all products
    process_all_products(output_dir='Product_Images_With_Barcodes')
    
    print("\nAll done! Check the output folder for your images.")
