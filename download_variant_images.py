# -*- coding: utf-8 -*-
"""
سكريبت لتحميل صور الـ Variants وتنظيمها في فولدرات
كل Product له فولدر خاص، وكل صورة تتسمى باسم اللون
"""

import os
import sys
import requests
from database import StockDatabase
from urllib.parse import urlparse
import re

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def sanitize_filename(name):
    """تنظيف اسم الملف من الأحرف الممنوعة"""
    # إزالة الأحرف الممنوعة في أسماء الملفات
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # إزالة المسافات الزائدة
    name = name.strip()
    return name

def get_image_extension(url):
    """استخراج امتداد الصورة من الـ URL"""
    parsed = urlparse(url)
    path = parsed.path
    ext = os.path.splitext(path)[1]
    
    # إذا لم يكن هناك امتداد، استخدم .jpg كافتراضي
    if not ext or ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
        ext = '.jpg'
    
    return ext

def download_variant_images(output_base_dir='Product_Images'):
    """
    تحميل صور الـ Variants وتنظيمها
    
    Args:
        output_base_dir: المجلد الأساسي الذي سيتم حفظ الصور فيه
    """
    # إنشاء المجلد الأساسي
    if not os.path.exists(output_base_dir):
        os.makedirs(output_base_dir)
        print(f"Created folder: {output_base_dir}")
    
    # الاتصال بقاعدة البيانات
    print("Connecting to database...")
    db = StockDatabase()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # جلب كل الـ Variants مع صورها
    print("Fetching images from database...")
    cursor.execute('''
        SELECT 
            bp.id as product_id,
            bp.product_code,
            b.brand_name,
            pt.type_name as product_type,
            c.color_name,
            ci.image_url,
            pv.id as variant_id
        FROM product_variants pv
        JOIN base_products bp ON pv.base_product_id = bp.id
        JOIN colors c ON pv.color_id = c.id
        LEFT JOIN brands b ON bp.brand_id = b.id
        LEFT JOIN product_types pt ON bp.product_type_id = pt.id
        LEFT JOIN color_images ci ON pv.id = ci.variant_id
        WHERE ci.image_url IS NOT NULL AND ci.image_url != ''
        ORDER BY bp.id, c.color_name
    ''')
    
    variants = cursor.fetchall()
    conn.close()
    
    if not variants:
        print("No images found in database!")
        return
    
    total_to_download = len(variants)
    print(f"Found {total_to_download} images to download")
    print("=" * 60)
    
    total_images = 0
    successful_downloads = 0
    failed_downloads = 0
    
    # تنظيم الـ Variants حسب المنتج
    current_product_id = None
    product_dir = None
    
    for idx, variant in enumerate(variants, 1):
        # استخراج البيانات من الـ tuple
        product_id = variant[0]
        product_code = variant[1] or 'Unknown'
        brand_name = variant[2] or 'Unknown'
        product_type = variant[3] or 'Unknown'
        color_name = variant[4] or 'Unknown'
        image_url = variant[5]
        
        # إنشاء مجلد جديد للمنتج إذا تغير
        if product_id != current_product_id:
            current_product_id = product_id
            
            # إنشاء اسم للمنتج (Product Code فقط)
            product_name = product_code
            product_name = sanitize_filename(product_name)
            
            # إنشاء مجلد للمنتج
            product_dir = os.path.join(output_base_dir, product_name)
            if not os.path.exists(product_dir):
                os.makedirs(product_dir)
            
            print(f"\n[{product_name}]")
        
        total_images += 1
        
        # تنظيف اسم اللون
        color_filename = sanitize_filename(color_name)
        
        # استخراج امتداد الصورة
        ext = get_image_extension(image_url)
        
        # اسم الملف النهائي
        image_filename = f"{color_filename}{ext}"
        image_path = os.path.join(product_dir, image_filename)
        
        # تحميل الصورة
        try:
            response = requests.get(image_url, timeout=10)
            response.raise_for_status()
            
            # حفظ الصورة
            with open(image_path, 'wb') as f:
                f.write(response.content)
            
            print(f"  [{idx}/{total_to_download}] OK: {color_name}")
            successful_downloads += 1
            
        except requests.exceptions.RequestException as e:
            print(f"  [{idx}/{total_to_download}] FAIL: {color_name} - {str(e)[:40]}")
            failed_downloads += 1
        except Exception as e:
            print(f"  [{idx}/{total_to_download}] ERROR: {color_name} - {str(e)[:40]}")
            failed_downloads += 1
    
    # ملخص النتائج
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY:")
    print(f"  Total images: {total_images}")
    print(f"  Successfully downloaded: {successful_downloads}")
    print(f"  Failed: {failed_downloads}")
    print(f"  Success rate: {(successful_downloads/total_images*100):.1f}%")
    print(f"  Output folder: {os.path.abspath(output_base_dir)}")
    print("=" * 60)

if __name__ == "__main__":
    print("Starting variant images download...")
    print("=" * 60)
    
    # يمكنك تغيير اسم المجلد هنا
    download_variant_images(output_base_dir='Product_Images')
    
    print("\nDownload completed!")
