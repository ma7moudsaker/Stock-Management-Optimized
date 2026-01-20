"""
Barcode Utilities Module
Handles barcode generation, image creation, and PDF label printing
"""

import os
import hashlib
import json
from datetime import datetime
from io import BytesIO

# Barcode generation
try:
    from barcode import EAN13
    from barcode.writer import ImageWriter
    BARCODE_AVAILABLE = True
except ImportError:
    BARCODE_AVAILABLE = False
    print("⚠️ python-barcode not installed. Run: pip install python-barcode")

# Image processing
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("⚠️ Pillow not installed. Run: pip install Pillow")

# PDF generation
try:
    from reportlab.lib.pagesizes import landscape
    from reportlab.lib.units import cm
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("⚠️ ReportLab not installed. Run: pip install reportlab")


class BarcodeGenerator:
    """Main class for barcode operations"""
    
    def __init__(self, output_dir='static/barcodes'):
        """
        Initialize barcode generator
        
        Args:
            output_dir: Directory to save barcode images
        """
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"✅ Created barcode directory: {output_dir}")
    
    def generate_ean13(self, product_code, color_name, variant_id=None):
        """
        Generate deterministic EAN-13 barcode
        
        Args:
            product_code: Product code (e.g., "SL-001")
            color_name: Color name (e.g., "Black")
            variant_id: Variant ID (used as fallback for collision)
        
        Returns:
            str: 13-digit EAN-13 barcode or None on error
        """
        try:
            # Create deterministic input string
            input_string = f"{product_code}|{color_name}"
            
            # Generate SHA-256 hash
            hash_obj = hashlib.sha256(input_string.encode('utf-8'))
            hash_hex = hash_obj.hexdigest()
            
            # Convert to integer and extract 12 digits
            hash_int = int(hash_hex, 16)
            base_12_digits = str(hash_int % 1000000000000).zfill(12)
            
            # Calculate EAN-13 check digit
            check_digit = self._calculate_ean13_check(base_12_digits)
            
            # Combine to create full barcode
            barcode = base_12_digits + str(check_digit)
            
            print(f"✅ Generated barcode: {barcode} for {product_code}-{color_name}")
            return barcode
            
        except Exception as e:
            print(f"❌ Error generating barcode: {e}")
            return None
    
    def _calculate_ean13_check(self, digits):
        """
        Calculate EAN-13 check digit
        
        Args:
            digits: First 12 digits of barcode
        
        Returns:
            int: Check digit (0-9)
        """
        if len(digits) != 12:
            raise ValueError("EAN-13 requires exactly 12 digits")
        
        # EAN-13 algorithm
        odd_sum = sum(int(digits[i]) for i in range(0, 12, 2))
        even_sum = sum(int(digits[i]) for i in range(1, 12, 2))
        
        total = odd_sum + (even_sum * 3)
        check_digit = (10 - (total % 10)) % 10
        
        return check_digit
    
    def create_barcode_image(self, barcode_number, product_code, color_name):
        """
        Create barcode image with labels
        
        Args:
            barcode_number: 13-digit EAN-13 barcode
            product_code: Product code
            color_name: Color name
        
        Returns:
            str: Path to saved image or None on error
        """
        if not BARCODE_AVAILABLE or not PIL_AVAILABLE:
            print("❌ Required libraries not available")
            return None
        
        try:
            # Generate filename
            safe_product = product_code.replace('/', '-').replace('\\', '-')
            safe_color = color_name.replace('/', '-').replace('\\', '-')
            filename = f"{safe_product}_{safe_color}_{barcode_number}.png"
            filepath = os.path.join(self.output_dir, filename)
            
            # Check if file already exists
            if os.path.exists(filepath):
                print(f"ℹ️ Barcode image already exists: {filename}")
                return filepath
            
            # Create barcode image using python-barcode
            writer = ImageWriter()
            writer.set_options({
                'module_width': 0.3,
                'module_height': 15.0,
                'quiet_zone': 6.5,
                'text_distance': 5.0,
                'font_size': 30,
                'write_text': True,
            })
            
            ean = EAN13(barcode_number, writer=writer)
            
            # Save without extension (library adds .png automatically)
            filepath_without_ext = filepath.replace('.png', '')
            ean.save(filepath_without_ext)
            
            # Add product code and color labels using Pillow
            self._add_labels_to_image(filepath, product_code, color_name)
            
            print(f"✅ Barcode image created: {filename}")
            return filepath
            
        except Exception as e:
            print(f"❌ Error creating barcode image: {e}")
            return None
    
    def _add_labels_to_image(self, image_path, product_code, color_name):
        """Add product code and color name labels to barcode image"""
        try:
            # Open existing barcode image
            img = Image.open(image_path)
            width, height = img.size
            
            # ✅ زوّد المساحة للنصوص - كانت 60px
            new_height = height + 120  # ← زودها من 60 لـ 120
            
            # Create new image with extra space for labels
            new_img = Image.new('RGB', (width, new_height), 'white')
            
            # Paste original barcode
            new_img.paste(img, (0, 0))
            
            # Draw labels
            draw = ImageDraw.Draw(new_img)
            
            # Try to load a font (fallback to default if not available)
            try:
                # ✅ كبّر الخط - كان 14
                font_large = ImageFont.truetype("arial.ttf", 32)      # ← للكود
                font_medium = ImageFont.truetype("arial.ttf", 28)     # ← للون
            except:
                font_large = ImageFont.load_default()
                font_medium = ImageFont.load_default()
            
            # ✅ Draw product code (أكبر وأوضح)
            text1 = f"{product_code}"
            bbox1 = draw.textbbox((0, 0), text1, font=font_large)
            text1_width = bbox1[2] - bbox1[0]
            draw.text(
                ((width - text1_width) / 2, height + 15),  # ← كانت height + 5
                text1, 
                fill='black', 
                font=font_large
            )
            
            # ✅ Draw color name (أكبر وأوضح)
            text2 = f"{color_name}"
            bbox2 = draw.textbbox((0, 0), text2, font=font_medium)
            text2_width = bbox2[2] - bbox2[0]
            draw.text(
                ((width - text2_width) / 2, height + 70),  # ← كانت height + 30
                text2, 
                fill='black', 
                font=font_medium
            )
            
            # Save updated image
            new_img.save(image_path)
            
        except Exception as e:
            print(f"⚠️ Could not add labels to image: {e}")
    
    def generate_complete_barcode(self, product_code, color_name, variant_id=None):
        """
        Generate barcode number and create image in one step
        
        Args:
            product_code: Product code
            color_name: Color name
            variant_id: Variant ID
        
        Returns:
            dict: {'barcode': str, 'image_path': str} or None on error
        """
        try:
            # Generate barcode number
            barcode_number = self.generate_ean13(product_code, color_name, variant_id)
            
            if not barcode_number:
                return None
            
            # Create barcode image
            image_path = self.create_barcode_image(barcode_number, product_code, color_name)
            
            if not image_path:
                return None
            
            return {
                'barcode': barcode_number,
                'image_path': image_path
            }
            
        except Exception as e:
            print(f"❌ Error in complete barcode generation: {e}")
            return None


class BarcodePrinter:
    """Class for printing barcode labels to PDF"""
    
    def __init__(self, page_width=5*cm, page_height=3*cm):
        """
        Initialize barcode printer
        
        Args:
            page_width: Label width (default 5cm)
            page_height: Label height (default 3cm)
        """
        self.page_width = page_width
        self.page_height = page_height
    
    def create_label_pdf(self, labels_data, output_path='barcode_labels.pdf'):
        """
        Create PDF with barcode labels
        
        Args:
            labels_data: List of dicts with barcode info
                [
                    {
                        'barcode_image': 'path/to/image.png',
                        'product_code': 'SL-001',
                        'color_name': 'Black',
                        'barcode_number': '1234567890123',
                        'quantity': 5
                    },
                    ...
                ]
            output_path: Output PDF file path
        
        Returns:
            str: Path to created PDF or None on error
        """
        if not REPORTLAB_AVAILABLE:
            print("❌ ReportLab not available")
            return None
        
        try:
            # Create PDF canvas
            c = canvas.Canvas(output_path, pagesize=(self.page_width, self.page_height))
            
            total_labels = 0
            
            for label_data in labels_data:
                quantity = label_data.get('quantity', 1)
                
                # Print multiple copies if quantity > 1
                for _ in range(quantity):
                    self._draw_label(c, label_data)
                    c.showPage()  # New page for each label
                    total_labels += 1
            
            c.save()
            
            print(f"✅ PDF created: {output_path} ({total_labels} labels)")
            return output_path
            
        except Exception as e:
            print(f"❌ Error creating PDF: {e}")
            return None
    
    def _draw_label(self, canvas_obj, label_data):
        """
        Draw a single label on canvas - BARCODE IMAGE ONLY (LARGE)
        
        Args:
            canvas_obj: ReportLab canvas object
            label_data: Label information dict
        """
        try:
            barcode_image = label_data.get('barcode_image')
            product_code = label_data.get('product_code', '')
            color_name = label_data.get('color_name', '')
            barcode_number = label_data.get('barcode_number', '')
            
            # Calculate positions (centered)
            width = self.page_width
            height = self.page_height
            
            # Draw barcode image (if exists)
            if barcode_image and os.path.exists(barcode_image):
                try:
                    img = ImageReader(barcode_image)
                    
                    # ✅ Make image LARGE - takes up almost entire label
                    img_width = width * 0.95   # 95% of page width
                    img_height = height * 0.95  # 95% of page height
                    
                    # Center the image
                    x = (width - img_width) / 2
                    y = (height - img_height) / 2
                    
                    canvas_obj.drawImage(
                        img, 
                        x, y, 
                        width=img_width, 
                        height=img_height, 
                        preserveAspectRatio=True
                    )
                    
                except Exception as e:
                    print(f"⚠️ Could not draw barcode image: {e}")
            
            # ❌ NO TEXT - Just the barcode image!
            # All text labels removed to avoid duplication
            
        except Exception as e:
            print(f"❌ Error drawing label: {e}")


# Helper functions for easy access

def generate_barcode_for_variant(product_code, color_name, variant_id=None, output_dir='static/barcodes'):
    """
    Convenience function to generate barcode for a variant
    
    Returns:
        dict: {'barcode': str, 'image_path': str} or None
    """
    generator = BarcodeGenerator(output_dir=output_dir)
    return generator.generate_complete_barcode(product_code, color_name, variant_id)


def create_barcode_labels_pdf(labels_data, output_path='barcode_labels.pdf'):
    """
    Convenience function to create PDF labels
    
    Returns:
        str: PDF path or None
    """
    printer = BarcodePrinter()
    return printer.create_label_pdf(labels_data, output_path)


def validate_ean13(barcode):
    """
    Validate EAN-13 barcode check digit
    
    Args:
        barcode: 13-digit barcode string
    
    Returns:
        bool: True if valid
    """
    if len(barcode) != 13 or not barcode.isdigit():
        return False
    
    try:
        # Calculate check digit for first 12 digits
        generator = BarcodeGenerator()
        calculated_check = generator._calculate_ean13_check(barcode[:12])
        
        # Compare with provided check digit
        return int(barcode[12]) == calculated_check
        
    except Exception as e:
        print(f"❌ Error validating barcode: {e}")
        return False


# Test function
if __name__ == '__main__':
    print("=== Barcode Utils Test ===\n")
    
    # Test barcode generation
    print("1. Testing barcode generation...")
    result = generate_barcode_for_variant("TEST-001", "Black", variant_id=1)
    
    if result:
        print(f"   Barcode: {result['barcode']}")
        print(f"   Image: {result['image_path']}")
        print(f"   Valid: {validate_ean13(result['barcode'])}")
    else:
        print("   ❌ Failed")
    
    print("\n2. Testing PDF creation...")
    if result:
        labels = [
            {
                'barcode_image': result['image_path'],
                'product_code': 'TEST-001',
                'color_name': 'Black',
                'barcode_number': result['barcode'],
                'quantity': 2
            }
        ]
        
        pdf_path = create_barcode_labels_pdf(labels, 'test_labels.pdf')
        if pdf_path:
            print(f"   ✅ PDF created: {pdf_path}")
        else:
            print("   ❌ Failed")
    
    print("\n=== Test Complete ===")
