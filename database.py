import sqlite3
import os
from urllib.parse import urlparse
from datetime import datetime
import requests
from urllib.parse import urlparse
import re
import json
try:
    import psycopg  # psycopg3
    from psycopg.rows import dict_row
    PSYCOPG_VERSION = 3
except ImportError:
    try:
        import psycopg2  # psycopg2
        from psycopg2.extras import RealDictCursor
        PSYCOPG_VERSION = 2
    except ImportError:
        PSYCOPG_VERSION = None


class StockDatabase:

    def __init__(self, db_name='stock_management.db'):
        if os.getenv('DATABASE_URL') and PSYCOPG_VERSION:
            self.db_type = 'postgresql'
            self.setup_postgresql()
        else:
            self.db_type = 'sqlite'
            self.db_name = db_name
        
        self.init_database()
    
    def get_connection(self):
        """الحصول على اتصال قاعدة البيانات"""
        if self.db_type == 'postgresql':
            if PSYCOPG_VERSION == 3:
                return psycopg.connect(**self.pg_config, row_factory=dict_row)
            else:
                return psycopg2.connect(**self.pg_config, cursor_factory=RealDictCursor)
        else:
            return sqlite3.connect(self.db_name, timeout=30.0)
   
   
    def setup_postgresql(self):
        """إعداد اتصال PostgreSQL"""
        database_url = os.getenv('DATABASE_URL')
        
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        
        parsed = urlparse(database_url)
        self.pg_config = {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path[1:],
            'user': parsed.username,
            'password': parsed.password,
        }
    
    def migrate_to_postgresql(self):
        """نقل البيانات من SQLite إلى PostgreSQL"""
        if self.db_type != 'postgresql':
            print("Migration only needed for PostgreSQL")
            return
        
        # فحص إذا كانت الجداول موجودة
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM brands")
            print("✅ Database already contains data")
            conn.close()
            return
        except:
            # الجداول غير موجودة، نحتاج إنشاؤها
            conn.close()
            self.add_default_data()
            print("✅ Default data added to PostgreSQL")

    def init_database(self):
        """إنشاء الجداول - متوافق مع SQLite و PostgreSQL"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # تحديد نوع البيانات حسب قاعدة البيانات
        if self.db_type == 'postgresql':
            id_type = 'SERIAL PRIMARY KEY'
            timestamp_type = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            decimal_type = 'DECIMAL(10,2)'
        else:
            id_type = 'INTEGER PRIMARY KEY'
            timestamp_type = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            decimal_type = 'DECIMAL(10,2)'
        
    
        # جدول البراندات
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS brands (
                id {id_type},
                brand_name TEXT UNIQUE NOT NULL,
                created_date {timestamp_type}
            )
        ''')
        
        # جدول الألوان
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS colors (
                id {id_type},
                color_name TEXT UNIQUE NOT NULL,
                color_code TEXT,
                created_date {timestamp_type}
            )
        ''')

                
        # جدول أنواع المنتجات
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS product_types (
                id {id_type},
                type_name TEXT UNIQUE NOT NULL,
                created_date {timestamp_type}
            )
        ''')
        
        # جدول فئات التجار
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS trader_categories (
                id {id_type},
                category_code TEXT UNIQUE NOT NULL,
                category_name TEXT NOT NULL,
                description TEXT,
                created_date {timestamp_type}
            )
        ''')
        
        # جدول الموردين
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS suppliers (
                id {id_type},
                supplier_name TEXT NOT NULL,
                contact_phone TEXT,
                contact_email TEXT,
                address TEXT,
                created_date {timestamp_type}
            )
        ''')
        
        # جدول Tags
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS tags (
                id {id_type},
                tag_name TEXT UNIQUE NOT NULL,
                tag_category TEXT,
                tag_color TEXT DEFAULT '#6c757d',
                description TEXT,
                created_date {timestamp_type}
            )
        ''')
        
        # جدول المنتجات الأساسية مع المقاس
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS base_products (
                id {id_type},
                product_code TEXT NOT NULL,
                brand_id INTEGER,
                product_type_id INTEGER,
                trader_category TEXT,
                product_size TEXT,
                wholesale_price DECIMAL(10,2),
                retail_price DECIMAL(10,2),
                supplier_id INTEGER,
                created_date {timestamp_type},
                FOREIGN KEY (brand_id) REFERENCES brands(id),
                FOREIGN KEY (product_type_id) REFERENCES product_types(id),
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
            )
        ''')
        
        # جدول المتغيرات (المنتجات بالألوان)
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS product_variants (
                id {id_type},
                base_product_id INTEGER,
                color_id INTEGER,
                current_stock INTEGER DEFAULT 0,
                created_date {timestamp_type},
                FOREIGN KEY (base_product_id) REFERENCES base_products(id),
                FOREIGN KEY (color_id) REFERENCES colors(id)
            )
        ''')
        
        # جدول صور الألوان (لينكات فقط)
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS color_images (
                id {id_type},
                variant_id INTEGER UNIQUE NOT NULL,
                image_url TEXT,
                created_date {timestamp_type},
                FOREIGN KEY (variant_id) REFERENCES product_variants(id) ON DELETE CASCADE
            )
        ''')

        
        # جدول ربط المنتجات بالـ Tags
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS product_tags (
                id {id_type},
                product_id INTEGER,
                tag_id INTEGER,
                created_date {timestamp_type},
                FOREIGN KEY (product_id) REFERENCES base_products(id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE,
                UNIQUE(product_id, tag_id)
            )
        ''')
        
        # إضافة عمود المقاس للمنتجات الموجودة (إذا لم يكن موجود)
        try:
            cursor.execute('ALTER TABLE base_products ADD COLUMN product_size TEXT')
        except sqlite3.OperationalError:
            pass  # العمود موجود بالفعل
        
                # === Stock Logs Table ===
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS stock_logs (
                id {id_type},
                operation_type TEXT NOT NULL,
                product_id INTEGER,
                variant_id INTEGER,
                product_code TEXT,
                brand_name TEXT,
                product_type TEXT,
                color_name TEXT,
                image_url TEXT,
                old_value INTEGER,
                new_value INTEGER,
                change_amount INTEGER,
                username TEXT DEFAULT 'Admin',
                notes TEXT,
                source_page TEXT,
                source_url TEXT,
                created_date {timestamp_type}
            )
        ''')
        
        # Create index for better performance
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_date ON stock_logs(created_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_operation ON stock_logs(operation_type)')
        except:
            pass

                # === USERS SYSTEM ===
        print("Creating users tables...")
        
        # Users table
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS users (
                id {id_type},
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT NOT NULL,
                active INTEGER DEFAULT 1,
                created_date {timestamp_type},
                last_login {timestamp_type}
            )
        ''')
        
        # Pages table (available pages in the system)
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS pages (
                id {id_type},
                page_key TEXT UNIQUE NOT NULL,
                page_name TEXT NOT NULL,
                page_category TEXT,
                page_url TEXT,
                description TEXT,
                display_order INTEGER DEFAULT 0
            )
        ''')
        
        # User Permissions table
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS user_permissions (
                id {id_type},
                user_id INTEGER NOT NULL,
                page_key TEXT NOT NULL,
                granted_by INTEGER,
                granted_date {timestamp_type},
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, page_key)
            )
        ''')
        
        # Create indexes
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_active ON users(active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_permissions_user ON user_permissions(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_permissions_page ON user_permissions(page_key)')
        except:
            pass
        
        # Activity Logs Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_variant_id INTEGER,
                product_code TEXT,
                color_name TEXT,
                action TEXT,
                description TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                quantity_changed INTEGER DEFAULT 0,
                FOREIGN KEY (product_variant_id) REFERENCES product_variants(id)
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stock_activity_timestamp 
            ON activity_logs(timestamp DESC)
        ''')

                # === BARCODE SYSTEM TABLES ===
        print("Creating barcode system tables...")

        # Barcodes table
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS barcodes (
            id {id_type},
            variant_id INTEGER UNIQUE NOT NULL,
            barcode_number TEXT UNIQUE NOT NULL,
            image_path TEXT,
            generated_at {timestamp_type},
            generated_by INTEGER,
            FOREIGN KEY (variant_id) REFERENCES product_variants(id) ON DELETE CASCADE,
            FOREIGN KEY (generated_by) REFERENCES users(id)
        )
        ''')

        # Create indexes for barcodes
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_barcodes_variant ON barcodes(variant_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_barcodes_number ON barcodes(barcode_number)')
        except:
            pass

        # Barcode Sessions table
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS barcode_sessions (
            id {id_type},
            user_id INTEGER NOT NULL,
            session_mode TEXT NOT NULL,
            items TEXT,
            created_at {timestamp_type},
            last_updated {timestamp_type},
            status TEXT DEFAULT 'active',
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        ''')

        # Create index for sessions
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_user ON barcode_sessions(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_status ON barcode_sessions(status)')
        except:
            pass

        print("✅ Barcode system tables created!")


        # Initialize default pages
        self._initialize_default_pages(cursor)
        

        conn.commit()
        conn.close()

        print("✅ Users system initialized!")
        print(f"✅ Database initialized using {self.db_type}")
    
    def _initialize_default_pages(self, cursor):
        """Initialize the pages table with all available pages"""
        pages_data = [
            # View Pages
            ('dashboard', 'Dashboard', 'View', '/dashboard', 'Main dashboard with statistics', 1),
            ('products', 'Products', 'View', '/products_new', 'View all products', 2),
            ('product_details', 'Product Details', 'View', '/product_details/<id>', 'View single product details', 3),
            ('activity_logs', 'Activity Logs', 'View', '/logs', 'View stock activity logs', 4),
            
            # Add Products
            ('add_product', 'Add Single Product', 'Add', '/add_product_new', 'Add one product at a time', 10),
            ('add_multiple', 'Add Multiple Products', 'Add', '/add_products_multi', 'Add multiple products at once', 11),
            ('bulk_upload', 'Bulk Upload Excel', 'Add', '/bulk_upload_excel', 'Upload products from Excel', 12),
            
            # Inventory
            ('bulk_inventory', 'Bulk Inventory', 'Inventory', '/inventory_management', 'Manage stock in bulk', 20),
            ('export_products', 'Export Products', 'Inventory', '/export_products', 'Export products to Excel/PDF', 21),
            
            # Edit
            ('edit_product', 'Edit Product', 'Edit', '/edit_product/<id>', 'Edit product information', 30),
            
            # Settings
            ('manage_brands', 'Manage Brands', 'Settings', '/manage_brands', 'Add/edit/delete brands', 40),
            ('manage_colors', 'Manage Colors', 'Settings', '/manage_colors', 'Add/edit/delete colors', 41),
            ('manage_product_types', 'Manage Product Types', 'Settings', '/manage_product_types', 'Add/edit/delete types', 42),
            ('manage_trader_categories', 'Manage Trader Categories', 'Settings', '/manage_trader_categories', 'Manage categories', 43),
            ('manage_tags', 'Manage Tags', 'Settings', '/manage_tags', 'Manage product tags', 44),
            
            # Admin
            ('backup_system', 'Backup System', 'Admin', '/admin/backup', 'Manage database backups', 50),
            ('user_management', 'User Management', 'Admin', '/user_management', 'Manage users and permissions', 51),

            # Barcode System
            ('barcode_system', 'Barcode System', 'Barcode', '/barcode/management', 'Complete barcode management system', 60),

        ]
        
        for page in pages_data:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO pages 
                    (page_key, page_name, page_category, page_url, description, display_order)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', page)
            except Exception as e:
                print(f"Error inserting page {page[0]}: {e}")

    # === USER MANAGEMENT FUNCTIONS ===

    def create_user(self, username, password, full_name, granted_by=0):
        """Create a new user"""
        try:
            from werkzeug.security import generate_password_hash
            
            password_hash = generate_password_hash(password)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO users (username, password_hash, full_name, active)
                VALUES (?, ?, ?, 1)
            ''', (username, password_hash, full_name))
            
            user_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            print(f"✅ User created: {username} (ID: {user_id})")
            return user_id
        except Exception as e:
            print(f"❌ Error creating user: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def get_user_by_username(self, username):
        """Get user by username"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
            user = cursor.fetchone()
            conn.close()
            
            if user:
                return {
                    'id': user[0],
                    'username': user[1],
                    'password_hash': user[2],
                    'full_name': user[3],
                    'active': user[4],
                    'created_date': user[5],
                    'last_login': user[6]
                }
            return None
        except Exception as e:
            print(f"❌ Error getting user: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def get_user_by_id(self, user_id):
        """Get user by ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
            user = cursor.fetchone()
            conn.close()
            
            if user:
                return {
                    'id': user[0],
                    'username': user[1],
                    'password_hash': user[2],
                    'full_name': user[3],
                    'active': user[4],
                    'created_date': user[5],
                    'last_login': user[6]
                }
            return None
        except Exception as e:
            print(f"❌ Error getting user: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def get_all_users(self):
        """Get all users"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users ORDER BY created_date DESC')
            users = cursor.fetchall()
            conn.close()
            return users
        except Exception as e:
            print(f"❌ Error getting users: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def update_user(self, user_id, username=None, full_name=None, password=None, active=None):
        """Update user information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if username:
                cursor.execute('UPDATE users SET username = ? WHERE id = ?', (username, user_id))
            
            if full_name:
                cursor.execute('UPDATE users SET full_name = ? WHERE id = ?', (full_name, user_id))
            
            if password:
                from werkzeug.security import generate_password_hash
                password_hash = generate_password_hash(password)
                cursor.execute('UPDATE users SET password_hash = ? WHERE id = ?', (password_hash, user_id))
            
            if active is not None:
                cursor.execute('UPDATE users SET active = ? WHERE id = ?', (active, user_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error updating user: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def delete_user(self, user_id):
        """Delete a user (and their permissions via CASCADE)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error deleting user: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def update_last_login(self, user_id):
        """Update user's last login timestamp"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?', (user_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error updating last login: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    # === PAGES MANAGEMENT ===

    def get_all_pages(self):
        """Get all available pages grouped by category"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT page_key, page_name, page_category, page_url, description, display_order
                FROM pages
                ORDER BY display_order, page_name
            ''')
            pages = cursor.fetchall()
            conn.close()
            
            # Group by category
            grouped = {}
            for page in pages:
                category = page[2] or 'Other'
                if category not in grouped:
                    grouped[category] = []
                grouped[category].append({
                    'page_key': page[0],
                    'page_name': page[1],
                    'page_category': page[2],
                    'page_url': page[3],
                    'description': page[4],
                    'display_order': page[5]
                })
            
            return grouped
        except Exception as e:
            print(f"❌ Error getting pages: {e}")
            if 'conn' in locals():
                conn.close()
            return {}

    def get_page_by_key(self, page_key):
        """Get a single page by key"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM pages WHERE page_key = ?', (page_key,))
            page = cursor.fetchone()
            conn.close()
            return page
        except Exception as e:
            print(f"❌ Error getting page: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    # === PERMISSIONS MANAGEMENT ===

    def grant_permission(self, user_id, page_key, granted_by=0):
        """Grant a user permission to access a page"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO user_permissions (user_id, page_key, granted_by)
                VALUES (?, ?, ?)
            ''', (user_id, page_key, granted_by))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error granting permission: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def revoke_permission(self, user_id, page_key):
        """Revoke a user's permission to access a page"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM user_permissions 
                WHERE user_id = ? AND page_key = ?
            ''', (user_id, page_key))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error revoking permission: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def set_user_permissions(self, user_id, page_keys, granted_by=0):
        """Set all permissions for a user (replaces existing)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Delete all existing permissions
            cursor.execute('DELETE FROM user_permissions WHERE user_id = ?', (user_id,))
            
            # Add new permissions
            for page_key in page_keys:
                cursor.execute('''
                    INSERT INTO user_permissions (user_id, page_key, granted_by)
                    VALUES (?, ?, ?)
                ''', (user_id, page_key, granted_by))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error setting permissions: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def get_user_permissions(self, user_id):
        """Get all page_keys a user has access to"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT page_key FROM user_permissions WHERE user_id = ?
            ''', (user_id,))
            permissions = [row[0] for row in cursor.fetchall()]
            conn.close()
            return permissions
        except Exception as e:
            print(f"❌ Error getting user permissions: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def user_has_permission(self, user_id, page_key):
        """Check if a user has permission to access a page"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM user_permissions 
                WHERE user_id = ? AND page_key = ?
            ''', (user_id, page_key))
            count = cursor.fetchone()[0]
            conn.close()
            return count > 0
        except Exception as e:
            print(f"❌ Error checking permission: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def get_users_with_permissions(self):
        """Get all users with their permission counts"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    u.id,
                    u.username,
                    u.full_name,
                    u.active,
                    u.created_date,
                    u.last_login,
                    COUNT(up.page_key) as permission_count
                FROM users u
                LEFT JOIN user_permissions up ON u.id = up.user_id
                GROUP BY u.id
                ORDER BY u.created_date DESC
            ''')
            users = cursor.fetchall()
            conn.close()
            return users
        except Exception as e:
            print(f"❌ Error getting users with permissions: {e}")
            if 'conn' in locals():
                conn.close()
            return []


    def add_stock_log(self, operation_type, product_id=None, variant_id=None, 
                  product_code='', brand_name='', product_type='', color_name='',
                  image_url='', old_value=None, new_value=None, 
                  username='Admin', notes='', source_page='', source_url=''):
        """تسجيل عملية في الـ Stock Logs"""
        try:
            change_amount = None
            if old_value is not None and new_value is not None:
                change_amount = new_value - old_value
            
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO stock_logs 
                (operation_type, product_id, variant_id, product_code, brand_name, 
                product_type, color_name, image_url, old_value, new_value, 
                change_amount, username, notes, source_page, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (operation_type, product_id, variant_id, product_code, brand_name,
                product_type, color_name, image_url, old_value, new_value,
                change_amount, username, notes, source_page, source_url))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error adding log: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def get_all_logs(self, limit=100, operation_filter=None, date_from=None, 
                    date_to=None, search_term=None):
        """جلب الـ Logs مع الفلاتر"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = 'SELECT * FROM stock_logs WHERE 1=1'
            params = []
            
            if operation_filter:
                query += ' AND operation_type = ?'
                params.append(operation_filter)
            
            if date_from:
                query += ' AND DATE(created_date) >= DATE(?)'
                params.append(date_from)
            
            if date_to:
                query += ' AND DATE(created_date) <= DATE(?)'
                params.append(date_to)
            
            if search_term:
                search_term_param = f'%{search_term}%'
                query += ''' AND (product_code LIKE ? OR brand_name LIKE ? 
                            OR color_name LIKE ? OR product_type LIKE ?)'''
                params.extend([search_term_param] * 4)
            
            query += ' ORDER BY created_date DESC LIMIT ?'
            params.append(limit)
            
            cursor.execute(query, params)
            logs = cursor.fetchall()
            conn.close()
            
            return logs
        except Exception as e:
            print(f"❌ Error getting logs: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def get_logs_stats(self):
        """إحصائيات الـ Logs"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Total logs
            cursor.execute('SELECT COUNT(*) FROM stock_logs')
            total_logs = cursor.fetchone()[0]
            
            # Logs today
            cursor.execute('''
                SELECT COUNT(*) FROM stock_logs 
                WHERE DATE(created_date) = DATE('now')
            ''')
            logs_today = cursor.fetchone()[0]
            
            # Total stock added (positive changes)
            cursor.execute('''
                SELECT COALESCE(SUM(change_amount), 0) FROM stock_logs 
                WHERE change_amount > 0
            ''')
            total_added = cursor.fetchone()[0]
            
            # Total stock removed (negative changes)
            cursor.execute('''
                SELECT COALESCE(SUM(ABS(change_amount)), 0) FROM stock_logs 
                WHERE change_amount < 0
            ''')
            total_removed = cursor.fetchone()[0]
            
            # Most active products (top 5)
            cursor.execute('''
                SELECT product_code, brand_name, COUNT(*) as count
                FROM stock_logs
                WHERE product_code IS NOT NULL
                GROUP BY product_code, brand_name
                ORDER BY count DESC
                LIMIT 5
            ''')
            most_active = cursor.fetchall()
            
            conn.close()
            
            return {
                'total_logs': total_logs,
                'logs_today': logs_today,
                'total_added': total_added,
                'total_removed': total_removed,
                'most_active': most_active
            }
        except Exception as e:
            print(f"❌ Error getting logs stats: {e}")
            if 'conn' in locals():
                conn.close()
            return {
                'total_logs': 0,
                'logs_today': 0,
                'total_added': 0,
                'total_removed': 0,
                'most_active': []
            }

    def export_logs_to_json(self, output_file):
        """Export all logs to JSON file for backup"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM stock_logs ORDER BY created_date DESC')
            logs = cursor.fetchall()
            conn.close()
            
            logs_data = []
            for log in logs:
                logs_data.append({
                    'id': log[0],
                    'operation_type': log[1],
                    'product_id': log[2],
                    'variant_id': log[3],
                    'product_code': log[4],
                    'brand_name': log[5],
                    'product_type': log[6],
                    'color_name': log[7],
                    'image_url': log[8],
                    'old_value': log[9],
                    'new_value': log[10],
                    'change_amount': log[11],
                    'username': log[12],
                    'notes': log[13],
                    'source_page': log[14],
                    'source_url': log[15],
                    'created_date': str(log[16]) if log[16] else None
                })
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(logs_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Exported {len(logs_data)} logs to {output_file}")
            return True
        except Exception as e:
            print(f"❌ Error exporting logs: {e}")
            return False

    def import_logs_from_json(self, json_file):
        """Import logs from JSON backup"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                logs_data = json.load(f)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            imported = 0
            for log in logs_data:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO stock_logs 
                        (operation_type, product_id, variant_id, product_code, brand_name,
                        product_type, color_name, image_url, old_value, new_value, 
                        change_amount, username, notes, source_page, source_url, created_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (log['operation_type'], log['product_id'], log['variant_id'],
                        log['product_code'], log['brand_name'], log['product_type'],
                        log['color_name'], log['image_url'], log['old_value'], 
                        log['new_value'], log['change_amount'], log['username'],
                        log['notes'], log['source_page'], log['source_url'],
                        log['created_date']))
                    imported += 1
                except:
                    continue
            
            conn.commit()
            conn.close()
            
            print(f"✅ Imported {imported} logs from {json_file}")
            return True
        except Exception as e:
            print(f"❌ Error importing logs: {e}")
            return False

    
    def add_default_data(self):
        """إضافة بيانات افتراضية مع Tags"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # إضافة فئات التجار
        categories = [('L', 'Category L', 'Trader Category L'), 
                     ('F', 'Category F', 'Trader Category F')]
        for cat in categories:
            cursor.execute('INSERT OR IGNORE INTO trader_categories (category_code, category_name, description) VALUES (?, ?, ?)', cat)
        
        # إضافة براندات افتراضية
        brands = ['Saint Laurent', 'Gucci', 'Louis Vuitton', 'Guess', 'Tommy Hilfiger', 'Karl Lagerfeld']
        for brand in brands:
            cursor.execute('INSERT OR IGNORE INTO brands (brand_name) VALUES (?)', (brand,))
        
        # إضافة ألوان افتراضية
        colors = [('Black', '#000000'), ('Brown', '#8B4513'), ('Red', '#FF0000'), 
                 ('White', '#FFFFFF'), ('Beige', '#F5F5DC'), ('Navy', '#000080'),
                 ('Gold', '#FFD700'), ('Silver', '#C0C0C0'), ('Pink', '#FFC0CB'), ('Blue', '#0000FF')]
        for color in colors:
            cursor.execute('INSERT OR IGNORE INTO colors (color_name, color_code) VALUES (?, ?)', color)
        
        # إضافة أنواع منتجات افتراضية
        types = ['Handbag', 'Wallet', 'Backpack', 'Clutch', 'Shoulder Bag', 'Tote Bag']
        for ptype in types:
            cursor.execute('INSERT OR IGNORE INTO product_types (type_name) VALUES (?)', (ptype,))
        
        # إضافة مورد افتراضي
        cursor.execute('INSERT OR IGNORE INTO suppliers (supplier_name, contact_phone) VALUES (?, ?)', 
                      ('Default Supplier', '01000000000'))
        
        # إضافة Tags افتراضية
        default_tags = [
            ('Small', 'size', '#28a745', 'Small size products'),
            ('Medium', 'size', '#ffc107', 'Medium size products'),
            ('Large', 'size', '#fd7e14', 'Large size products'),
            ('XL', 'size', '#dc3545', 'Extra Large size products'),
            ('Sale', 'status', '#dc3545', 'Products on sale'),
            ('New Arrival', 'status', '#28a745', 'New products'),
            ('Limited Edition', 'status', '#6f42c1', 'Limited edition products'),
            ('Valentine\'s', 'occasion', '#e83e8c', 'Valentine\'s Day collection'),
            ('Christmas', 'occasion', '#dc3545', 'Christmas collection'),
            ('Summer', 'season', '#fd7e14', 'Summer collection'),
            ('Winter', 'season', '#6c757d', 'Winter collection'),
            ('Leather', 'material', '#8B4513', 'Leather products'),
            ('Canvas', 'material', '#6c757d', 'Canvas products'),
            ('Casual', 'style', '#17a2b8', 'Casual style'),
            ('Formal', 'style', '#343a40', 'Formal style')
        ]
        
        for tag in default_tags:
            cursor.execute('INSERT OR IGNORE INTO tags (tag_name, tag_category, tag_color, description) VALUES (?, ?, ?, ?)', tag)
        
        conn.commit()
        conn.close()
        print("✅ Default data with enhanced tags added!")
    
    # وظائف إدارة البراندات
    def get_all_brands(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM brands ORDER BY brand_name')
        brands = cursor.fetchall()
        conn.close()
        return brands
    
    def add_brand(self, brand_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT INTO brands (brand_name) VALUES (?)', (brand_name,))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def update_brand(self, brand_id, new_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('UPDATE brands SET brand_name = ? WHERE id = ?', (new_name, brand_id))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def delete_brand(self, brand_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM base_products WHERE brand_id = ?', (brand_id,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                conn.close()
                return False, "Cannot delete brand - it's used by existing products"
            
            cursor.execute('DELETE FROM brands WHERE id = ?', (brand_id,))
            conn.commit()
            conn.close()
            return True, "Brand deleted successfully"
        except Exception as e:
            conn.close()
            return False, str(e)
    
    def get_brand_by_id(self, brand_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM brands WHERE id = ?', (brand_id,))
        brand = cursor.fetchone()
        conn.close()
        return brand
    
    # وظائف إدارة الألوان
    def get_all_colors(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM colors ORDER BY color_name')
        colors = cursor.fetchall()
        conn.close()
        return colors
    
    def add_color(self, color_name, color_code='#FFFFFF'):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT INTO colors (color_name, color_code) VALUES (?, ?)', (color_name, color_code))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def update_color(self, color_id, new_name, new_code):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('UPDATE colors SET color_name = ?, color_code = ? WHERE id = ?', 
                          (new_name, new_code, color_id))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def delete_color(self, color_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM product_variants WHERE color_id = ?', (color_id,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                conn.close()
                return False, "Cannot delete color - it's used by existing products"
            
            cursor.execute('DELETE FROM colors WHERE id = ?', (color_id,))
            conn.commit()
            conn.close()
            return True, "Color deleted successfully"
        except Exception as e:
            conn.close()
            return False, str(e)
    
    def get_color_by_id(self, color_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM colors WHERE id = ?', (color_id,))
        color = cursor.fetchone()
        conn.close()
        return color
    
    def get_color_name_by_id(self, color_id):
        """جلب اسم اللون بالـ ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT color_name FROM colors WHERE id = ?', (color_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    # وظائف إدارة أنواع المنتجات
    def get_all_product_types(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM product_types ORDER BY type_name')
        types = cursor.fetchall()
        conn.close()
        return types
    
    def add_product_type(self, type_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT INTO product_types (type_name) VALUES (?)', (type_name,))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def update_product_type(self, type_id, new_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('UPDATE product_types SET type_name = ? WHERE id = ?', (new_name, type_id))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def delete_product_type(self, type_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM base_products WHERE product_type_id = ?', (type_id,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                conn.close()
                return False, "Cannot delete product type - it's used by existing products"
            
            cursor.execute('DELETE FROM product_types WHERE id = ?', (type_id,))
            conn.commit()
            conn.close()
            return True, "Product type deleted successfully"
        except Exception as e:
            conn.close()
            return False, str(e)
    
    def get_product_type_by_id(self, type_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM product_types WHERE id = ?', (type_id,))
        ptype = cursor.fetchone()
        conn.close()
        return ptype
    
    # وظائف إدارة فئات التجار
    def get_all_trader_categories(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM trader_categories ORDER BY category_code')
        categories = cursor.fetchall()
        conn.close()
        return categories

    def add_trader_category(self, category_code, category_name, description=''):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT INTO trader_categories (category_code, category_name, description) VALUES (?, ?, ?)',
                           (category_code, category_name, description))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False

    def update_trader_category(self, category_id, new_code, new_name, new_description):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('UPDATE trader_categories SET category_code = ?, category_name = ?, description = ? WHERE id = ?',
                           (new_code, new_name, new_description, category_id))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False

    def delete_trader_category(self, category_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM base_products WHERE trader_category = (SELECT category_code FROM trader_categories WHERE id = ?)', (category_id,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                conn.close()
                return False, "Cannot delete category - it's used by existing products"
            
            cursor.execute('DELETE FROM trader_categories WHERE id = ?', (category_id,))
            conn.commit()
            conn.close()
            return True, "Category deleted successfully"
        except Exception as e:
            conn.close()
            return False, str(e)

    def get_trader_category_by_id(self, category_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM trader_categories WHERE id = ?', (category_id,))
        category = cursor.fetchone()
        conn.close()
        return category

    # وظائف إدارة Tags
    def get_all_tags(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tags ORDER BY tag_category, tag_name')
        tags = cursor.fetchall()
        conn.close()
        return tags
    
    def get_tags_by_category(self, category=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        if category:
            cursor.execute('SELECT * FROM tags WHERE tag_category = ? ORDER BY tag_name', (category,))
        else:
            cursor.execute('SELECT DISTINCT tag_category FROM tags ORDER BY tag_category')
        tags = cursor.fetchall()
        conn.close()
        return tags
    
    def add_tag(self, tag_name, tag_category='general', tag_color='#6c757d', description=''):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT INTO tags (tag_name, tag_category, tag_color, description) VALUES (?, ?, ?, ?)',
                           (tag_name, tag_category, tag_color, description))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def update_tag(self, tag_id, new_name, new_category, new_color, new_description):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('UPDATE tags SET tag_name = ?, tag_category = ?, tag_color = ?, description = ? WHERE id = ?',
                           (new_name, new_category, new_color, new_description, tag_id))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False
    
    def delete_tag(self, tag_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM product_tags WHERE tag_id = ?', (tag_id,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                conn.close()
                return False, "Cannot delete tag - it's used by existing products"
            
            cursor.execute('DELETE FROM tags WHERE id = ?', (tag_id,))
            conn.commit()
            conn.close()
            return True, "Tag deleted successfully"
        except Exception as e:
            conn.close()
            return False, str(e)
    
    def get_tag_by_id(self, tag_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tags WHERE id = ?', (tag_id,))
        tag = cursor.fetchone()
        conn.close()
        return tag
    
    def add_product_tags(self, product_id, tag_ids):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('DELETE FROM product_tags WHERE product_id = ?', (product_id,))
            
            for tag_id in tag_ids:
                cursor.execute('INSERT INTO product_tags (product_id, tag_id) VALUES (?, ?)', 
                              (product_id, tag_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            conn.close()
            return False
    
    def get_product_tags(self, product_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT t.* FROM tags t
            JOIN product_tags pt ON t.id = pt.tag_id
            WHERE pt.product_id = ?
            ORDER BY t.tag_category, t.tag_name
        ''', (product_id,))
        tags = cursor.fetchall()
        conn.close()
        return tags

    # وظائف إدارة المنتجات مع النظام المحدث
    def add_base_product_with_variants(self, product_code, brand_id, product_type_id, 
                                     trader_category, product_size, wholesale_price, retail_price, 
                                     color_ids, tag_ids=None, initial_stock=0, supplier_id=1):
        """إضافة منتج أساسي مع متغيرات الألوان والمقاس والـ Tags"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO base_products (product_code, brand_id, product_type_id, 
                                         trader_category, product_size, wholesale_price, retail_price, supplier_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (product_code, brand_id, product_type_id, trader_category, 
                  product_size, wholesale_price, retail_price, supplier_id))
            
            base_product_id = cursor.lastrowid
            
            for color_id in color_ids:
                cursor.execute('''
                    INSERT INTO product_variants (base_product_id, color_id, current_stock)
                    VALUES (?, ?, ?)
                ''', (base_product_id, color_id, initial_stock))
            
            if tag_ids:
                for tag_id in tag_ids:
                    cursor.execute('''
                        INSERT INTO product_tags (product_id, tag_id)
                        VALUES (?, ?)
                    ''', (base_product_id, tag_id))
            
            conn.commit()
            conn.close()
            return True, base_product_id
        except Exception as e:
            conn.close()
            return False, str(e)
    
    def get_all_products_with_details(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                bp.id,
                bp.product_code,
                b.brand_name,
                pt.type_name,
                bp.trader_category,
                bp.product_size,
                bp.wholesale_price,
                bp.retail_price,
                s.supplier_name,
                GROUP_CONCAT(DISTINCT c.color_name) as colors,
                SUM(pv.current_stock) as total_stock,
                bp.created_date,
                GROUP_CONCAT(DISTINCT t.tag_name) as tags
            FROM base_products bp
            LEFT JOIN brands b ON bp.brand_id = b.id
            LEFT JOIN product_types pt ON bp.product_type_id = pt.id
            LEFT JOIN suppliers s ON bp.supplier_id = s.id
            LEFT JOIN product_variants pv ON bp.id = pv.base_product_id
            LEFT JOIN colors c ON pv.color_id = c.id
            LEFT JOIN product_tags ptags ON bp.id = ptags.product_id
            LEFT JOIN tags t ON ptags.tag_id = t.id
            GROUP BY bp.id
            ORDER BY bp.created_date DESC
        ''')
        
        products = cursor.fetchall()
        conn.close()
        return products
    
    def check_product_exists(self, product_code, brand_id, trader_category):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id FROM base_products 
            WHERE product_code = ? AND brand_id = ? AND trader_category = ?
        ''', (product_code, brand_id, trader_category))
        
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    def search_products(self, search_term=''):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if search_term:
            search_term = f'%{search_term}%'
            cursor.execute('''
                SELECT DISTINCT
                    bp.id,
                    bp.product_code,
                    b.brand_name,
                    pt.type_name,
                    bp.trader_category,
                    bp.product_size,
                    bp.wholesale_price,
                    bp.retail_price,
                    s.supplier_name,
                    GROUP_CONCAT(DISTINCT c.color_name) as colors,
                    SUM(pv.current_stock) as total_stock,
                    bp.created_date,
                    GROUP_CONCAT(DISTINCT t.tag_name) as tags
                FROM base_products bp
                LEFT JOIN brands b ON bp.brand_id = b.id
                LEFT JOIN product_types pt ON bp.product_type_id = pt.id
                LEFT JOIN suppliers s ON bp.supplier_id = s.id
                LEFT JOIN product_variants pv ON bp.id = pv.base_product_id
                LEFT JOIN colors c ON pv.color_id = c.id
                LEFT JOIN product_tags ptags ON bp.id = ptags.product_id
                LEFT JOIN tags t ON ptags.tag_id = t.id
                WHERE bp.product_code LIKE ? 
                   OR b.brand_name LIKE ?
                   OR c.color_name LIKE ?
                   OR bp.trader_category LIKE ?
                   OR pt.type_name LIKE ?
                   OR bp.product_size LIKE ?
                   OR t.tag_name LIKE ?
                GROUP BY bp.id
                ORDER BY bp.created_date DESC
            ''', (search_term, search_term, search_term, search_term, search_term, search_term, search_term))
        else:
            cursor.execute('''
                SELECT 
                    bp.id,
                    bp.product_code,
                    b.brand_name,
                    pt.type_name,
                    bp.trader_category,
                    bp.product_size,
                    bp.wholesale_price,
                    bp.retail_price,
                    s.supplier_name,
                    GROUP_CONCAT(DISTINCT c.color_name) as colors,
                    SUM(pv.current_stock) as total_stock,
                    bp.created_date,
                    GROUP_CONCAT(DISTINCT t.tag_name) as tags
                FROM base_products bp
                LEFT JOIN brands b ON bp.brand_id = b.id
                LEFT JOIN product_types pt ON bp.product_type_id = pt.id
                LEFT JOIN suppliers s ON bp.supplier_id = s.id
                LEFT JOIN product_variants pv ON bp.id = pv.base_product_id
                LEFT JOIN colors c ON pv.color_id = c.id
                LEFT JOIN product_tags ptags ON bp.id = ptags.product_id
                LEFT JOIN tags t ON ptags.tag_id = t.id
                GROUP BY bp.id
                ORDER BY bp.created_date DESC
            ''')
        
        products = cursor.fetchall()
        conn.close()
        return products
    
    def get_product_details(self, product_id):
        """جلب تفاصيل منتج واحد مع مخزون كل لون والمقاس والـ Tags"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                bp.id, bp.product_code, bp.trader_category, bp.product_size, 
                bp.wholesale_price, bp.retail_price, bp.created_date,
                b.brand_name, pt.type_name, s.supplier_name
            FROM base_products bp
            LEFT JOIN brands b ON bp.brand_id = b.id
            LEFT JOIN product_types pt ON bp.product_type_id = pt.id
            LEFT JOIN suppliers s ON bp.supplier_id = s.id
            WHERE bp.id = ?
        ''', (product_id,))
        
        product = cursor.fetchone()
        
        if not product:
            conn.close()
            return None
        
        cursor.execute('''
            SELECT 
                pv.id as variant_id, c.id as color_id, c.color_name, c.color_code,
                pv.current_stock, ci.image_url
            FROM product_variants pv
            JOIN colors c ON pv.color_id = c.id
            LEFT JOIN color_images ci ON pv.id = ci.variant_id
            WHERE pv.base_product_id = ?
            ORDER BY pv.current_stock DESC, c.color_name
        ''', (product_id,))
        
        color_stocks = cursor.fetchall()
        
        product_tags = self.get_product_tags(product_id)
        
        total_stock = sum([stock[4] for stock in color_stocks])
        
        conn.close()
        
        return {
            'product': product,
            'color_stocks': color_stocks,
            'total_stock': total_stock,
            'tags': product_tags
        }
    
    def delete_product(self, product_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM product_tags WHERE product_id = ?', (product_id,))
            cursor.execute('DELETE FROM product_variants WHERE base_product_id = ?', (product_id,))
            cursor.execute('DELETE FROM base_products WHERE id = ?', (product_id,))
            
            conn.commit()
            conn.close()
            return True, "Product deleted successfully"
        except Exception as e:
            conn.close()
            return False, str(e)
    
        # وظائف إدارة الصور المحدثة
    def add_color_image(self, variant_id, image_url):
        """إضافة/تحديث لينك صورة اللون"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO color_images (variant_id, image_url)
                VALUES (?, ?)
            ''', (variant_id, image_url))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error adding color image: {e}")
            conn.close()
            return False

    
    def get_product_images_with_details(self, product_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                bp.id, bp.product_code, bp.trader_category, bp.product_size, 
                bp.wholesale_price, bp.retail_price, bp.created_date,
                b.brand_name, pt.type_name, s.supplier_name,
                pv.id as variant_id, c.id as color_id, c.color_name, c.color_code,
                pv.current_stock, ci.image_url
            FROM base_products bp
            LEFT JOIN brands b ON bp.brand_id = b.id
            LEFT JOIN product_types pt ON bp.product_type_id = pt.id
            LEFT JOIN suppliers s ON bp.supplier_id = s.id
            LEFT JOIN product_variants pv ON bp.id = pv.base_product_id
            LEFT JOIN colors c ON pv.color_id = c.id
            LEFT JOIN color_images ci ON pv.id = ci.variant_id
            WHERE bp.id = ?
            ORDER BY pv.current_stock DESC, c.color_name
        ''', (product_id,))
        
        results = cursor.fetchall()
        conn.close()
        return results
    
    def get_product_main_image(self, product_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ci.image_url
            FROM product_variants pv
            LEFT JOIN color_images ci ON pv.id = ci.variant_id
            WHERE pv.base_product_id = ? AND ci.image_url IS NOT NULL
            ORDER BY pv.current_stock DESC, pv.id ASC
            LIMIT 1
        ''', (product_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def get_products_with_color_images(self, search_term=''):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if search_term:
            search_term = f'%{search_term}%'
            cursor.execute('''
                SELECT DISTINCT
                    bp.id, bp.product_code, b.brand_name, pt.type_name,
                    bp.trader_category, bp.product_size, bp.wholesale_price, bp.retail_price,
                    s.supplier_name, bp.created_date
                FROM base_products bp
                LEFT JOIN brands b ON bp.brand_id = b.id
                LEFT JOIN product_types pt ON bp.product_type_id = pt.id
                LEFT JOIN suppliers s ON bp.supplier_id = s.id
                LEFT JOIN product_variants pv ON bp.id = pv.base_product_id
                LEFT JOIN colors c ON pv.color_id = c.id
                LEFT JOIN product_tags ptags ON bp.id = ptags.product_id
                LEFT JOIN tags t ON ptags.tag_id = t.id
                WHERE bp.product_code LIKE ? OR b.brand_name LIKE ? OR c.color_name LIKE ? 
                   OR bp.product_size LIKE ? OR t.tag_name LIKE ?
                ORDER BY bp.created_date DESC
            ''', (search_term, search_term, search_term, search_term, search_term))
        else:
            cursor.execute('''
                SELECT 
                    bp.id, bp.product_code, b.brand_name, pt.type_name,
                    bp.trader_category, bp.product_size, bp.wholesale_price, bp.retail_price,
                    s.supplier_name, bp.created_date
                FROM base_products bp
                LEFT JOIN brands b ON bp.brand_id = b.id
                LEFT JOIN product_types pt ON bp.product_type_id = pt.id
                LEFT JOIN suppliers s ON bp.supplier_id = s.id
                ORDER BY bp.created_date DESC
            ''')
        
        products = cursor.fetchall()
        
        products_with_images = []
        for product in products:
            cursor.execute('''
                SELECT 
                    pv.id as variant_id,
                    c.color_name,
                    c.color_code,
                    pv.current_stock,
                    ci.image_url
                FROM product_variants pv
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN color_images ci ON pv.id = ci.variant_id
                WHERE pv.base_product_id = ?
                ORDER BY pv.current_stock DESC
            ''', (product[0],))
            
            color_data = cursor.fetchall()
            total_stock = sum([cd[3] for cd in color_data])
            
            product_tags = self.get_product_tags(product[0])
            
            colors_with_images = []
            for cd in color_data:
                colors_with_images.append({
                    'variant_id': cd[0],
                    'name': cd[1],
                    'code': cd[2],
                    'stock': cd[3],
                    'image_url': cd[4]
                })
            
            product_data = list(product) + [colors_with_images, total_stock, product_tags]
            products_with_images.append(product_data)
        
        conn.close()
        return products_with_images

    # وظائف إضافة منتجات متعددة دفعة واحدة
    def add_multiple_products_batch(self, products_data):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        success_count = 0
        failed_products = []
        
        try:
            for product_data in products_data:
                try:
                    if self.check_product_exists(
                        product_data['product_code'], 
                        product_data['brand_id'], 
                        product_data['trader_category']
                    ):
                        failed_products.append({
                            'product': product_data,
                            'error': 'Product already exists'
                        })
                        continue
                    
                    cursor.execute('''
                        INSERT INTO base_products (product_code, brand_id, product_type_id, 
                                                 trader_category, product_size, wholesale_price, retail_price, supplier_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        product_data['product_code'],
                        product_data['brand_id'],
                        product_data['product_type_id'],
                        product_data['trader_category'],
                        product_data.get('product_size', ''),
                        product_data['wholesale_price'],
                        product_data['retail_price'],
                        product_data.get('supplier_id', 1)
                    ))
                    
                    base_product_id = cursor.lastrowid
                    
                    for color_id in product_data['color_ids']:
                        cursor.execute('''
                            INSERT INTO product_variants (base_product_id, color_id, current_stock)
                            VALUES (?, ?, ?)
                        ''', (base_product_id, color_id, product_data.get('initial_stock', 0)))
                    
                    if 'tag_ids' in product_data and product_data['tag_ids']:
                        for tag_id in product_data['tag_ids']:
                            cursor.execute('''
                                INSERT INTO product_tags (product_id, tag_id)
                                VALUES (?, ?)
                            ''', (base_product_id, tag_id))
                    
                    success_count += 1
                    
                except Exception as e:
                    failed_products.append({
                        'product': product_data,
                        'error': str(e)
                    })
                    continue
            
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'success_count': success_count,
                'failed_count': len(failed_products),
                'failed_products': failed_products
            }
            
        except Exception as e:
            conn.rollback()
            conn.close()
            return {
                'success': False,
                'error': str(e),
                'success_count': 0,
                'failed_count': len(products_data)
            }


    def get_all_products_for_inventory(self, search_term='', brand_filter='', category_filter=''):
        """جلب جميع المنتجات للجرد الشامل مع تفاصيل كل لون والصور"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        base_query = '''
            SELECT 
                bp.id, bp.product_code, b.brand_name, pt.type_name,
                bp.trader_category, bp.product_size, bp.wholesale_price, 
                bp.retail_price, bp.created_date
            FROM base_products bp
            LEFT JOIN brands b ON bp.brand_id = b.id
            LEFT JOIN product_types pt ON bp.product_type_id = pt.id
            WHERE 1=1
        '''
        
        params = []
        
        if search_term:
            base_query += ' AND (bp.product_code LIKE ? OR b.brand_name LIKE ? OR bp.product_size LIKE ?)'
            search_param = f'%{search_term}%'
            params.extend([search_param, search_param, search_param])
        
        if brand_filter:
            base_query += ' AND b.brand_name = ?'
            params.append(brand_filter)
        
        if category_filter:
            base_query += ' AND bp.trader_category = ?'
            params.append(category_filter)
        
        base_query += ' ORDER BY b.brand_name, bp.product_code'
        
        cursor.execute(base_query, params)
        products = cursor.fetchall()
        
        inventory_data = []
        for product in products:
            cursor.execute('''
                SELECT pv.id, c.id, c.color_name, c.color_code, pv.current_stock, ci.image_url
                FROM product_variants pv
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN color_images ci ON pv.id = ci.variant_id
                WHERE pv.base_product_id = ?
                ORDER BY c.color_name
            ''', (product[0],))
            
            color_variants = cursor.fetchall()
            total_stock = sum([cv[4] for cv in color_variants])
            product_tags = self.get_product_tags(product[0])
            
            inventory_data.append({
                'product': product,
                'color_variants': color_variants,
                'total_stock': total_stock,
                'tags': product_tags
            })
        
        conn.close()
        return inventory_data

    def get_inventory_summary(self):
        """جلب ملخص المخزون للتقارير"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT bp.id) as total_products,
                COUNT(pv.id) as total_variants,
                SUM(pv.current_stock) as total_stock,
                SUM(CASE WHEN pv.current_stock = 0 THEN 1 ELSE 0 END) as out_of_stock_variants,
                SUM(CASE WHEN pv.current_stock > 0 AND pv.current_stock <= 5 THEN 1 ELSE 0 END) as low_stock_variants
            FROM base_products bp
            LEFT JOIN product_variants pv ON bp.id = pv.base_product_id
        ''')
        
        summary = cursor.fetchone()
        conn.close()
        
        return {
            'total_products': summary[0] or 0,
            'total_variants': summary[1] or 0,
            'total_stock': summary[2] or 0,
            'out_of_stock_variants': summary[3] or 0,
            'low_stock_variants': summary[4] or 0
        }

    def get_brands_for_filter(self):
        """جلب البراندات للفلترة"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT brand_name FROM brands ORDER BY brand_name')
        brands = [row[0] for row in cursor.fetchall()]
        conn.close()
        return brands

    def get_categories_for_filter(self):
        """جلب فئات التجار للفلترة"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT category_code FROM trader_categories ORDER BY category_code')
        categories = [row[0] for row in cursor.fetchall()]
        conn.close()
        return categories

    def bulk_update_inventory(self, stock_updates):
        """تحديث المخزون بشكل جماعي"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        success_count = 0
        failed_updates = []
        
        try:
            for update in stock_updates:
                try:
                    cursor.execute('''
                        UPDATE product_variants 
                        SET current_stock = ? 
                        WHERE id = ?
                    ''', (update['new_stock'], update['variant_id']))
                    
                    success_count += 1
                    
                except Exception as e:
                    failed_updates.append({
                        'variant_id': update['variant_id'],
                        'error': str(e)
                    })
            
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'updated_count': success_count,
                'failed_count': len(failed_updates),
                'failed_updates': failed_updates
            }
            
        except Exception as e:
            conn.rollback()
            conn.close()
            return {
                'success': False,
                'error': str(e)
            }


    def bulk_add_products_from_excel_enhanced(self, excel_data):
        """إضافة منتجات من Excel مع تحسين الأداء ومعالجة أخطاء البيانات المختلطة"""
        
        # تحسين أداء SQLite
        if self.db_type == 'sqlite':
            import sqlite3
            conn = sqlite3.connect(self.db_name, timeout=60.0, isolation_level='DEFERRED')
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA cache_size=-64000')  # 64MB cache
            cursor = conn.cursor()
        else:
            conn = self.get_connection()
            cursor = conn.cursor()
        
        success_count = 0
        failed_products = []
        processed_products = {}
        created_brands = []
        created_colors = []
        created_types = []
        
        # معالجة البيانات في دفعات أكبر
        BATCH_SIZE = 100
        
        try:
            for batch_start in range(0, len(excel_data), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(excel_data))
                batch_data = excel_data[batch_start:batch_end]
                
                print(f"🔄 معالجة الدفعة {batch_start + 1}-{batch_end} من إجمالي {len(excel_data)}")
                
                for index, row in enumerate(batch_data, batch_start + 1):
                    try:
                        # تحويل جميع القيم إلى نصوص مع حماية من النوع float
                        product_code = str(row.get('Product Code', '')).strip()
                        brand_name = str(row.get('Brand Name', '')).strip()
                        product_type_name = str(row.get('Product Type', '')).strip()
                        color_name = str(row.get('Color Name', '')).strip()
                        category = str(row.get('Category', '')).strip()
                        size = str(row.get('Size', '')).strip()
                        
                        # معالجة الأسعار والأرقام بحذر
                        try:
                            wholesale_price = float(row.get('Wholesale Price', 0))
                            retail_price = float(row.get('Retail Price', 0))
                            initial_stock = int(row.get('Stock', 0))
                        except (ValueError, TypeError):
                            wholesale_price = 0.0
                            retail_price = 0.0
                            initial_stock = 0
                        
                        tags = str(row.get('Tags', '')).strip()
                        image_url = str(row.get('Image URL', '')).strip()
                        
                        # التحقق من البيانات الأساسية المطلوبة
                        if not product_code or not brand_name or not color_name:
                            failed_products.append({
                                'row': index,
                                'product_code': product_code,
                                'error': 'Missing required data (Product Code, Brand Name, or Color Name)'
                            })
                            continue
                        
                        # البحث عن أو إنشاء Brand
                        cursor.execute('SELECT id FROM brands WHERE brand_name = ?', (brand_name,))
                        brand_result = cursor.fetchone()
                        if not brand_result:
                            cursor.execute('INSERT INTO brands (brand_name) VALUES (?)', (brand_name,))
                            brand_id = cursor.lastrowid
                            if brand_name not in created_brands:
                                created_brands.append(brand_name)
                        else:
                            brand_id = brand_result[0]
                        
                        # البحث عن أو إنشاء Product Type
                        cursor.execute('SELECT id FROM product_types WHERE type_name = ?', (product_type_name,))
                        type_result = cursor.fetchone()
                        if not type_result:
                            cursor.execute('INSERT INTO product_types (type_name) VALUES (?)', (product_type_name,))
                            product_type_id = cursor.lastrowid
                            if product_type_name not in created_types:
                                created_types.append(product_type_name)
                        else:
                            product_type_id = type_result[0]
                        
                        # البحث عن أو إنشاء Color
                        cursor.execute('SELECT id FROM colors WHERE color_name = ?', (color_name,))
                        color_result = cursor.fetchone()
                        if not color_result:
                            default_color_codes = {
                                'black': '#000000', 'white': '#FFFFFF', 'red': '#FF0000',
                                'blue': '#0000FF', 'green': '#008000', 'yellow': '#FFFF00',
                                'brown': '#8B4513', 'pink': '#FFC0CB', 'purple': '#800080',
                                'orange': '#FFA500', 'gray': '#808080', 'grey': '#808080',
                                'gold': '#FFD700', 'silver': '#C0C0C0', 'navy': '#000080',
                                'beige': '#F5F5DC', 'maroon': '#800000'
                            }
                            color_code = default_color_codes.get(color_name.lower(), '#FFFFFF')
                            cursor.execute('INSERT INTO colors (color_name, color_code) VALUES (?, ?)',
                                        (color_name, color_code))
                            color_id = cursor.lastrowid
                            if color_name not in created_colors:
                                created_colors.append(color_name)
                        else:
                            color_id = color_result[0]
                        
                        # إنشاء أو تحديث المنتج الأساسي
                        product_key = f"{product_code}_{brand_id}_{category}"
                        
                        if product_key not in processed_products:
                            cursor.execute('''
                                SELECT id FROM base_products
                                WHERE product_code = ? AND brand_id = ? AND trader_category = ?
                            ''', (product_code, brand_id, category))
                            
                            existing = cursor.fetchone()
                            
                            if existing:
                                base_product_id = existing[0]
                                cursor.execute('''
                                    UPDATE base_products
                                    SET product_type_id = ?, product_size = ?,
                                        wholesale_price = ?, retail_price = ?
                                    WHERE id = ?
                                ''', (product_type_id, size, wholesale_price, retail_price, base_product_id))
                            else:
                                cursor.execute('''
                                    INSERT INTO base_products
                                    (product_code, brand_id, product_type_id, trader_category,
                                    product_size, wholesale_price, retail_price, supplier_id)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                                ''', (product_code, brand_id, product_type_id, category,
                                    size, wholesale_price, retail_price))
                                base_product_id = cursor.lastrowid
                            
                            processed_products[product_key] = base_product_id
                        else:
                            base_product_id = processed_products[product_key]
                        
                        # إضافة أو تحديث variant اللون
                        cursor.execute('''
                            SELECT id, current_stock FROM product_variants
                            WHERE base_product_id = ? AND color_id = ?
                        ''', (base_product_id, color_id))
                        
                        variant_result = cursor.fetchone()
                        
                        if variant_result:
                            variant_id = variant_result[0]
                            cursor.execute('''
                                UPDATE product_variants
                                SET current_stock = ?
                                WHERE id = ?
                            ''', (initial_stock, variant_id))
                        else:
                            cursor.execute('''
                                INSERT INTO product_variants (base_product_id, color_id, current_stock)
                                VALUES (?, ?, ?)
                            ''', (base_product_id, color_id, initial_stock))
                            variant_id = cursor.lastrowid
                        
                        # حفظ لينك الصورة مباشرة (داخل نفس الـ transaction)
                        if image_url and image_url.lower() not in ['nan', 'none', '']:
                            try:
                                cursor.execute('''
                                    INSERT OR REPLACE INTO color_images (variant_id, image_url)
                                    VALUES (?, ?)
                                ''', (variant_id, image_url))
                            except Exception as img_error:
                                print(f"⚠️ فشل حفظ صورة {color_name}: {img_error}")
                        
                        # معالجة Tags
                        if tags and tags.lower() not in ['nan', 'none', '']:
                            tag_list = [t.strip() for t in tags.split(',') if t.strip()]
                            for tag_name in tag_list:
                                cursor.execute('SELECT id FROM tags WHERE tag_name = ?', (tag_name,))
                                tag_result = cursor.fetchone()
                                if tag_result:
                                    tag_id = tag_result[0]
                                    try:
                                        cursor.execute('''
                                            INSERT OR IGNORE INTO product_tags (product_id, tag_id)
                                            VALUES (?, ?)
                                        ''', (base_product_id, tag_id))
                                    except:
                                        pass
                        
                        success_count += 1
                        
                    except Exception as e:
                        failed_products.append({
                            'row': index,
                            'product_code': product_code if 'product_code' in locals() else 'Unknown',
                            'error': str(e)
                        })
                        continue
                
                # Commit بعد كل دفعة
                conn.commit()
                print(f"✅ تم حفظ الدفعة {batch_start + 1}-{batch_end}")
            
            conn.close()
            
            return {
                'success': True,
                'success_count': success_count,
                'failed_count': len(failed_products),
                'failed_products': failed_products,
                'created_brands': created_brands,
                'created_colors': created_colors,
                'created_types': created_types
            }
            
        except Exception as e:
            conn.rollback()
            conn.close()
            return {
                'success': False,
                'error': str(e),
                'success_count': 0,
                'failed_count': len(excel_data)
            }
    
    # ==========================================
    # DASHBOARD ANALYTICS
    # ==========================================
    
    def get_total_stock_quantity(self):
        """Get total quantity of all products in stock"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT SUM(current_stock) 
                FROM product_variants
            ''')
            
            total = cursor.fetchone()[0]
            conn.close()
            
            return total if total else 0
            
        except Exception as e:
            print(f"Error getting total stock quantity: {e}")
            return 0
    
    
    def get_stock_quantity_trend(self, days=30):
        """Get stock quantity trend for the last X days"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get daily stock snapshots from activity logs
            cursor.execute('''
                SELECT 
                    DATE(timestamp) as date,
                    SUM(CASE 
                        WHEN action = 'Added' OR action = 'Stock Increased' 
                        THEN CAST(REPLACE(REPLACE(description, 'Stock increased by ', ''), 'Added ', '') AS INTEGER)
                        ELSE 0 
                    END) as added,
                    SUM(CASE 
                        WHEN action = 'Removed' OR action = 'Stock Decreased' 
                        THEN CAST(REPLACE(REPLACE(description, 'Stock decreased by ', ''), 'Removed ', '') AS INTEGER)
                        ELSE 0 
                    END) as removed
                FROM activity_logs
                WHERE DATE(timestamp) >= DATE('now', '-' || ? || ' days')
                AND action IN ('Added', 'Removed', 'Stock Increased', 'Stock Decreased')
                GROUP BY DATE(timestamp)
                ORDER BY date ASC
            ''', (days,))
            
            results = cursor.fetchall()
            conn.close()
            
            # Calculate cumulative stock
            from datetime import datetime, timedelta
            
            dates = []
            quantities = []
            
            # Get current stock as baseline
            current_stock = self.get_total_stock_quantity()
            
            if results:
                # Generate all dates for last 30 days
                today = datetime.now().date()
                all_dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days-1, -1, -1)]
                
                # Create dict from results
                results_dict = {row[0]: (row[1] or 0, row[2] or 0) for row in results}
                
                # Work backwards from today
                running_stock = current_stock
                
                for date in reversed(all_dates):
                    dates.insert(0, date)
                    quantities.insert(0, running_stock)
                    
                    if date in results_dict:
                        added, removed = results_dict[date]
                        running_stock = running_stock - added + removed
            else:
                # No activity logs, just show current stock
                from datetime import datetime, timedelta
                today = datetime.now().date()
                dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days-1, -1, -1)]
                quantities = [current_stock] * days
            
            return {'dates': dates, 'quantities': quantities}
            
        except Exception as e:
            print(f"Error getting stock quantity trend: {e}")
            return {'dates': [], 'quantities': []}
    
    
    def get_stock_value_trend(self, days=30):
        """Get stock value trend for the last X days"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get current total value
            cursor.execute('''
                SELECT SUM(bp.wholesale_price * pv.current_stock)
                FROM product_variants pv
                JOIN base_products bp ON pv.base_product_id = bp.id
            ''')
            
            current_value = cursor.fetchone()[0] or 0
            
            # Get daily value changes from activity logs
            cursor.execute('''
                SELECT 
                    DATE(al.timestamp) as date,
                    SUM(CASE 
                        WHEN al.action IN ('Added', 'Stock Increased')
                        THEN bp.wholesale_price * CAST(
                            REPLACE(REPLACE(al.description, 'Stock increased by ', ''), 'Added ', '') 
                            AS INTEGER)
                        WHEN al.action IN ('Removed', 'Stock Decreased')
                        THEN -1 * bp.wholesale_price * CAST(
                            REPLACE(REPLACE(al.description, 'Stock decreased by ', ''), 'Removed ', '') 
                            AS INTEGER)
                        ELSE 0
                    END) as value_change
                FROM activity_logs al
                JOIN base_products bp ON al.product_code = bp.product_code
                WHERE DATE(al.timestamp) >= DATE('now', '-' || ? || ' days')
                AND al.action IN ('Added', 'Removed', 'Stock Increased', 'Stock Decreased')
                GROUP BY DATE(al.timestamp)
                ORDER BY date ASC
            ''', (days,))
            
            results = cursor.fetchall()
            conn.close()
            
            # Calculate cumulative values
            from datetime import datetime, timedelta
            
            dates = []
            values = []
            
            if results:
                # Generate all dates
                today = datetime.now().date()
                all_dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days-1, -1, -1)]
                
                # Create dict from results
                results_dict = {row[0]: (row[1] or 0) for row in results}
                
                # Work backwards from today
                running_value = current_value
                
                for date in reversed(all_dates):
                    dates.insert(0, date)
                    values.insert(0, round(running_value, 2))
                    
                    if date in results_dict:
                        running_value = running_value - results_dict[date]
            else:
                # No activity logs
                from datetime import datetime, timedelta
                today = datetime.now().date()
                dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days-1, -1, -1)]
                values = [round(current_value, 2)] * days
            
            return {'dates': dates, 'values': values}
            
        except Exception as e:
            print(f"Error getting stock value trend: {e}")
            return {'dates': [], 'values': []}
    
    
    def get_total_stock_value(self):
        """Get total value of all stock (wholesale price * quantity)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT SUM(bp.wholesale_price * pv.current_stock)
                FROM product_variants pv
                JOIN base_products bp ON pv.base_product_id = bp.id
            ''')
            
            total = cursor.fetchone()[0]
            conn.close()
            
            return float(total) if total else 0.0
            
        except Exception as e:
            print(f"Error getting total stock value: {e}")
            return 0.0
    
    
    def get_most_updated_products(self, limit=10, days=30):
        """Get products with most stock updates (by variant)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    al.product_code,
                    b.brand_name,
                    al.color_name,
                    SUM(CASE WHEN al.action IN ('Added', 'Stock Increased') THEN 1 ELSE 0 END) as additions,
                    SUM(CASE WHEN al.action IN ('Removed', 'Stock Decreased') THEN 1 ELSE 0 END) as removals,
                    MAX(al.timestamp) as last_update
                FROM activity_logs al
                JOIN base_products bp ON al.product_code = bp.product_code
                JOIN brands b ON bp.brand_id = b.id
                WHERE DATE(al.timestamp) >= DATE('now', '-' || ? || ' days')
                AND al.action IN ('Added', 'Removed', 'Stock Increased', 'Stock Decreased')
                GROUP BY al.product_code, al.color_name
                HAVING (additions + removals) > 0
                ORDER BY (additions + removals) DESC, last_update DESC
                LIMIT ?
            ''', (days, limit))
            
            results = cursor.fetchall()
            conn.close()
            
            return results
            
        except Exception as e:
            print(f"Error getting most updated products: {e}")
            return []
    
    
    def get_top_brands(self, limit=5):
        """Get top brands by product count"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    b.brand_name,
                    COUNT(DISTINCT bp.id) as product_count
                FROM brands b
                JOIN base_products bp ON b.id = bp.brand_id
                GROUP BY b.id
                ORDER BY product_count DESC
                LIMIT ?
            ''', (limit,))
            
            results = cursor.fetchall()
            conn.close()
            
            return results
            
        except Exception as e:
            print(f"Error getting top brands: {e}")
            return []
    
    def get_top_products_by_stock(self, limit=10):
        """Get top products by total stock quantity (sum of all variants)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    bp.product_code,
                    b.brand_name,
                    pt.type_name,
                    tc.category_name,
                    SUM(pv.current_stock) as total_stock
                FROM base_products bp
                JOIN brands b ON bp.brand_id = b.id
                JOIN product_types pt ON bp.product_type_id = pt.id
                JOIN trader_categories tc ON bp.trader_category = tc.category_code
                JOIN product_variants pv ON bp.id = pv.base_product_id
                GROUP BY bp.id
                HAVING total_stock > 0
                ORDER BY total_stock DESC
                LIMIT ?
            ''', (limit,))
            
            results = cursor.fetchall()
            conn.close()
            
            return results
            
        except Exception as e:
            print(f"Error getting top products by stock: {e}")
            return []
 
    
    def get_products_by_category(self):
        """Get product distribution by trader category"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    tc.category_name,
                    COUNT(bp.id) as product_count
                FROM trader_categories tc
                JOIN base_products bp ON tc.category_code = bp.trader_category
                GROUP BY tc.category_code
                ORDER BY product_count DESC
            ''')
            
            results = cursor.fetchall()
            conn.close()
            
            return results
            
        except Exception as e:
            print(f"Error getting products by category: {e}")
            return []
    
    
    def get_active_system_counts(self):
        """Get counts of active system entities (only those with products)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Active brands
            cursor.execute('''
                SELECT COUNT(DISTINCT bp.brand_id)
                FROM base_products bp
            ''')
            brands_count = cursor.fetchone()[0]
            
            # Active categories
            cursor.execute('''
                SELECT COUNT(DISTINCT bp.trader_category)
                FROM base_products bp
            ''')
            categories_count = cursor.fetchone()[0]
            
            # Active product types
            cursor.execute('''
                SELECT COUNT(DISTINCT bp.product_type_id)
                FROM base_products bp
            ''')
            types_count = cursor.fetchone()[0]
            
            # Active colors
            cursor.execute('''
                SELECT COUNT(DISTINCT pv.color_id)
                FROM product_variants pv
            ''')
            colors_count = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'brands': brands_count,
                'categories': categories_count,
                'types': types_count,
                'colors': colors_count
            }
            
        except Exception as e:
            print(f"Error getting active system counts: {e}")
            return {
                'brands': 0,
                'categories': 0,
                'types': 0,
                'colors': 0
            }
        

    # ====================================================================
    # BARCODE SYSTEM FUNCTIONS
    # ====================================================================

    # === Basic Barcode Operations ===

    def create_barcode(self, variant_id, barcode_number, image_path, user_id=None):
        """Create a new barcode entry"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO barcodes (variant_id, barcode_number, image_path, generated_by)
            VALUES (?, ?, ?, ?)
            ''', (variant_id, barcode_number, image_path, user_id))
            
            barcode_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            print(f"✅ Barcode created: {barcode_number} for variant {variant_id}")
            return barcode_id
            
        except Exception as e:
            print(f"❌ Error creating barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def get_barcode_by_variant(self, variant_id):
        """Get barcode for a specific variant"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT b.*, u.full_name as generated_by_name
            FROM barcodes b
            LEFT JOIN users u ON b.generated_by = u.id
            WHERE b.variant_id = ?
            ''', (variant_id,))
            
            barcode = cursor.fetchone()
            conn.close()
            return barcode
            
        except Exception as e:
            print(f"❌ Error getting barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def get_barcode_by_number(self, barcode_number):
        """Get barcode details by barcode number"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    b.id,                    -- 0: barcode_id
                    b.variant_id,            -- 1: variant_id
                    b.barcode_number,        -- 2: barcode_number
                    b.image_path,            -- 3: barcode_image_path
                    pv.current_stock,        -- 4: current_stock
                    bp.product_code,         -- 5: product_code
                    br.brand_name,           -- 6: brand_name
                    pt.type_name,            -- 7: product_type
                    c.color_name,            -- 8: color_name
                    c.color_code,            -- 9: color_code
                    ci.image_url,            -- 10: product_image_url
                    bp.wholesale_price,      -- 11: wholesale_price
                    bp.retail_price,         -- 12: retail_price
                    bp.product_size          -- 13: product_size
                FROM barcodes b
                JOIN product_variants pv ON b.variant_id = pv.id
                JOIN base_products bp ON pv.base_product_id = bp.id
                JOIN brands br ON bp.brand_id = br.id
                JOIN product_types pt ON bp.product_type_id = pt.id
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN color_images ci ON pv.id = ci.variant_id
                WHERE b.barcode_number = ?
            """, (barcode_number,))
            
            result = cursor.fetchone()
            conn.close()
            return result
            
        except Exception as e:
            print(f"❌ Error looking up barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def barcode_exists(self, barcode_number):
        """Check if barcode already exists"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM barcodes WHERE barcode_number = ?', (barcode_number,))
            count = cursor.fetchone()[0]
            conn.close()
            
            return count > 0
            
        except Exception as e:
            print(f"❌ Error checking barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def delete_barcode(self, variant_id):
        """Delete barcode for a variant"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM barcodes WHERE variant_id = ?', (variant_id,))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error deleting barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    # === Variants WITHOUT Barcode ===

    def get_variants_without_barcode(self, search='', brand_filter='', type_filter='', color_filter='', limit=50, offset=0, in_stock_only=False):
        """Get variants without barcodes with optional stock filter"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    pv.id as variant_id,
                    bp.product_code,
                    br.brand_name,
                    pt.type_name,
                    c.color_name,
                    c.color_code,
                    pv.current_stock,
                    ci.image_url,
                    bp.product_size
                FROM product_variants pv
                JOIN base_products bp ON pv.base_product_id = bp.id
                JOIN brands br ON bp.brand_id = br.id
                JOIN product_types pt ON bp.product_type_id = pt.id
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN color_images ci ON pv.id = ci.variant_id
                LEFT JOIN barcodes b ON pv.id = b.variant_id
                WHERE b.id IS NULL
            """
            params = []
            
            if in_stock_only:
                query += " AND pv.current_stock > 0"
            
            if search:
                query += """ AND (bp.product_code LIKE ? OR br.brand_name LIKE ? 
                            OR c.color_name LIKE ?)"""
                search_param = f'%{search}%'
                params.extend([search_param, search_param, search_param])
            
            if brand_filter:
                query += " AND br.brand_name = ?"
                params.append(brand_filter)
            
            if type_filter:
                query += " AND pt.type_name = ?"
                params.append(type_filter)
            
            if color_filter:
                query += " AND c.color_name = ?"
                params.append(color_filter)
            
            query += " ORDER BY br.brand_name, bp.product_code, c.color_name"
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            variants = cursor.fetchall()
            conn.close()
            
            return variants
            
        except Exception as e:
            print(f"❌ Error getting variants without barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def count_variants_without_barcode(self, search='', brand_filter='', type_filter='', color_filter='', in_stock_only=False):
        """Count variants without barcodes with optional stock filter"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT COUNT(*)
                FROM product_variants pv
                JOIN base_products bp ON pv.base_product_id = bp.id
                JOIN brands br ON bp.brand_id = br.id
                JOIN product_types pt ON bp.product_type_id = pt.id
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN barcodes b ON pv.id = b.variant_id
                WHERE b.id IS NULL
            """
            params = []
            
            if in_stock_only:
                query += " AND pv.current_stock > 0"
            
            if search:
                query += """ AND (bp.product_code LIKE ? OR br.brand_name LIKE ? 
                            OR c.color_name LIKE ?)"""
                search_param = f'%{search}%'
                params.extend([search_param, search_param, search_param])
            
            if brand_filter:
                query += " AND br.brand_name = ?"
                params.append(brand_filter)
            
            if type_filter:
                query += " AND pt.type_name = ?"
                params.append(type_filter)
            
            if color_filter:
                query += " AND c.color_name = ?"
                params.append(color_filter)
            
            cursor.execute(query, params)
            count = cursor.fetchone()[0]
            conn.close()
            
            return count
            
        except Exception as e:
            print(f"❌ Error counting variants without barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return 0

    # === Variants WITH Barcode ===

    def get_variants_with_barcode(self, search='', brand_filter='', type_filter='', 
                                   color_filter='', limit=50, offset=0):
        """Get all variants that have barcodes"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = '''
            SELECT 
                pv.id as variant_id,
                bp.product_code,
                br.brand_name,
                pt.type_name,
                c.color_name,
                c.color_code,
                pv.current_stock,
                ci.image_url,
                bp.product_size,
                b.barcode_number,
                b.image_path as barcode_image,
                b.generated_at
            FROM product_variants pv
            JOIN base_products bp ON pv.base_product_id = bp.id
            JOIN brands br ON bp.brand_id = br.id
            JOIN product_types pt ON bp.product_type_id = pt.id
            JOIN colors c ON pv.color_id = c.id
            LEFT JOIN color_images ci ON pv.id = ci.variant_id
            JOIN barcodes b ON pv.id = b.variant_id
            WHERE 1=1
            '''
            
            params = []
            
            if search:
                query += ' AND (bp.product_code LIKE ? OR br.brand_name LIKE ? OR c.color_name LIKE ? OR b.barcode_number LIKE ?)'
                search_param = f'%{search}%'
                params.extend([search_param, search_param, search_param, search_param])
            
            if brand_filter:
                query += ' AND br.brand_name = ?'
                params.append(brand_filter)
            
            if type_filter:
                query += ' AND pt.type_name = ?'
                params.append(type_filter)
            
            if color_filter:
                query += ' AND c.color_name = ?'
                params.append(color_filter)
            
            query += ' ORDER BY br.brand_name, bp.product_code, c.color_name LIMIT ? OFFSET ?'
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            variants = cursor.fetchall()
            conn.close()
            
            return variants
            
        except Exception as e:
            print(f"❌ Error getting variants with barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def count_variants_with_barcode(self, search='', brand_filter='', type_filter='', color_filter=''):
        """Count variants with barcodes"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = '''
            SELECT COUNT(*)
            FROM product_variants pv
            JOIN base_products bp ON pv.base_product_id = bp.id
            JOIN brands br ON bp.brand_id = br.id
            JOIN product_types pt ON bp.product_type_id = pt.id
            JOIN colors c ON pv.color_id = c.id
            JOIN barcodes b ON pv.id = b.variant_id
            WHERE 1=1
            '''
            
            params = []
            
            if search:
                query += ' AND (bp.product_code LIKE ? OR br.brand_name LIKE ? OR c.color_name LIKE ? OR b.barcode_number LIKE ?)'
                search_param = f'%{search}%'
                params.extend([search_param, search_param, search_param, search_param])
            
            if brand_filter:
                query += ' AND br.brand_name = ?'
                params.append(brand_filter)
            
            if type_filter:
                query += ' AND pt.type_name = ?'
                params.append(type_filter)
            
            if color_filter:
                query += ' AND c.color_name = ?'
                params.append(color_filter)
            
            cursor.execute(query, params)
            count = cursor.fetchone()[0]
            conn.close()
            
            return count
            
        except Exception as e:
            print(f"❌ Error counting variants: {e}")
            if 'conn' in locals():
                conn.close()
            return 0

    # === Barcode Image Management ===

    def get_barcodes_with_image_status(self, search='', brand_filter='', type_filter='', 
                                        color_filter='', image_filter='all', limit=50, offset=0):
        """Get variants with barcodes and check image status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    pv.id as variant_id,
                    bp.product_code,
                    br.brand_name,
                    pt.type_name,
                    c.color_name,
                    c.color_code,
                    pv.current_stock,
                    ci.image_url,
                    bp.product_size,
                    b.barcode_number,
                    b.image_path as barcode_image,
                    b.generated_at
                FROM product_variants pv
                JOIN base_products bp ON pv.base_product_id = bp.id
                JOIN brands br ON bp.brand_id = br.id
                JOIN product_types pt ON bp.product_type_id = pt.id
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN color_images ci ON pv.id = ci.variant_id
                JOIN barcodes b ON pv.id = b.variant_id
                WHERE 1=1
            """
            params = []
            
            if search:
                query += """ AND (bp.product_code LIKE ? OR br.brand_name LIKE ? 
                            OR c.color_name LIKE ? OR b.barcode_number LIKE ?)"""
                search_param = f'%{search}%'
                params.extend([search_param, search_param, search_param, search_param])
            
            if brand_filter:
                query += " AND br.brand_name = ?"
                params.append(brand_filter)
            
            if type_filter:
                query += " AND pt.type_name = ?"
                params.append(type_filter)
            
            if color_filter:
                query += " AND c.color_name = ?"
                params.append(color_filter)
            
            query += " ORDER BY br.brand_name, bp.product_code, c.color_name"
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            variants = cursor.fetchall()
            conn.close()
            
            # Check image existence for each variant
            results = []
            for v in variants:
                image_path = v[10]  # barcode_image
                
                # Check if image file exists
                image_exists = False
                if image_path and os.path.exists(image_path):
                    if os.path.getsize(image_path) > 1024:  # > 1KB
                        image_exists = True
                
                # Apply image filter
                if image_filter == 'with_image' and not image_exists:
                    continue
                elif image_filter == 'missing_image' and image_exists:
                    continue
                
                results.append({
                    'variant_id': v[0],
                    'product_code': v[1],
                    'brand_name': v[2],
                    'type_name': v[3],
                    'color_name': v[4],
                    'color_code': v[5],
                    'current_stock': v[6],
                    'image_url': v[7],
                    'product_size': v[8],
                    'barcode_number': v[9],
                    'barcode_image': v[10],
                    'generated_at': v[11],
                    'image_exists': image_exists
                })
            
            return results
            
        except Exception as e:
            print(f"❌ Error getting barcodes with image status: {e}")
            if 'conn' in locals():
                conn.close()
            return []

    def count_barcode_image_status(self):
        """Count barcodes by image status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT barcode_number, image_path
                FROM barcodes
            """)
            barcodes = cursor.fetchall()
            conn.close()
            
            total = len(barcodes)
            with_image = 0
            missing_image = 0
            
            for barcode in barcodes:
                image_path = barcode[1]
                
                # Check if image exists
                if image_path and os.path.exists(image_path):
                    if os.path.getsize(image_path) > 1024:
                        with_image += 1
                    else:
                        missing_image += 1
                else:
                    missing_image += 1
            
            return {
                'total': total,
                'with_image': with_image,
                'missing_image': missing_image
            }
            
        except Exception as e:
            print(f"❌ Error counting image status: {e}")
            return {'total': 0, 'with_image': 0, 'missing_image': 0}

    def update_barcode_image_path(self, variant_id, image_path):
        """Update barcode image path"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE barcodes 
                SET image_path = ?
                WHERE variant_id = ?
            """, (image_path, variant_id))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error updating barcode image path: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    # === Statistics ===

    def get_barcode_stats(self):
        """Get barcode system statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Total variants
            cursor.execute('SELECT COUNT(*) FROM product_variants')
            total_variants = cursor.fetchone()[0]
            
            # Variants with barcode
            cursor.execute('SELECT COUNT(*) FROM barcodes')
            with_barcode = cursor.fetchone()[0]
            
            # Variants without barcode
            without_barcode = total_variants - with_barcode
            
            # Completion rate
            completion_rate = (with_barcode / total_variants * 100) if total_variants > 0 else 0
            
            conn.close()
            
            return {
                'total_variants': total_variants,
                'with_barcode': with_barcode,
                'without_barcode': without_barcode,
                'completion_rate': round(completion_rate, 1)
            }
            
        except Exception as e:
            print(f"❌ Error getting barcode stats: {e}")
            if 'conn' in locals():
                conn.close()
            return {
                'total_variants': 0,
                'with_barcode': 0,
                'without_barcode': 0,
                'completion_rate': 0
            }

    # === Session Management ===

    def create_scan_session(self, user_id, mode):
        """Create a new barcode scanning session"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO barcode_sessions (user_id, session_mode, items, status)
            VALUES (?, ?, ?, 'active')
            ''', (user_id, mode, '[]'))
            
            session_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            print(f"✅ Scan session created: {session_id} for user {user_id}")
            return session_id
            
        except Exception as e:
            print(f"❌ Error creating session: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def get_active_session(self, user_id):
        """Get active session for a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM barcode_sessions
            WHERE user_id = ? AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            ''', (user_id,))
            
            session = cursor.fetchone()
            conn.close()
            return session
            
        except Exception as e:
            print(f"❌ Error getting active session: {e}")
            if 'conn' in locals():
                conn.close()
            return None

    def update_session_items(self, session_id, items_json):
        """Update session items"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE barcode_sessions
            SET items = ?, last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (items_json, session_id))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error updating session: {e}")
            if 'conn' in locals():
                conn.close()
            return False
        

    def add_item_to_session(self, session_id, variant_id):
        """Add item to session or increment quantity if exists"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get current session items
            cursor.execute('SELECT items FROM barcode_sessions WHERE id = ?', (session_id,))
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                return False
            
            # Parse current items
            items_json = result[0] or '[]'
            items = json.loads(items_json) if items_json else []
            
            # Check if item already exists
            item_found = False
            for item in items:
                if item.get('variant_id') == variant_id:
                    item['quantity'] = item.get('quantity', 1) + 1
                    item_found = True
                    break
            
            # If not found, add new item
            if not item_found:
                items.append({
                    'variant_id': variant_id,
                    'quantity': 1
                })
            
            # Update session
            cursor.execute('''
            UPDATE barcode_sessions 
            SET items = ?, last_updated = CURRENT_TIMESTAMP 
            WHERE id = ?
            ''', (json.dumps(items), session_id))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error adding item to session: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def get_variant_by_barcode(self, barcode):
        """Get variant ID by barcode number"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT pv.id, bp.product_code, br.brand_name, pt.type_name, 
                c.color_name, c.color_code, pv.current_stock, ci.image_url, bp.product_size
            FROM barcodes b
            JOIN product_variants pv ON b.variant_id = pv.id
            JOIN base_products bp ON pv.base_product_id = bp.id
            JOIN brands br ON bp.brand_id = br.id
            JOIN product_types pt ON bp.product_type_id = pt.id
            JOIN colors c ON pv.color_id = c.id
            LEFT JOIN color_images ci ON pv.id = ci.variant_id
            WHERE b.barcode_number = ?
            LIMIT 1
            ''', (barcode,))
            
            variant = cursor.fetchone()
            conn.close()
            return variant
            
        except Exception as e:
            print(f"❌ Error getting variant by barcode: {e}")
            if 'conn' in locals():
                conn.close()
            return None
        
    def get_session_items_with_details(self, session_id):
        """Get session items with full product details"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get session items
            cursor.execute('SELECT items FROM barcode_sessions WHERE id = ?', (session_id,))
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                return []
            
            items_json = result[0] or '[]'
            items = json.loads(items_json) if items_json else []
            
            # Get details for each item
            detailed_items = []
            for item in items:
                variant_id = item.get('variant_id')
                quantity = item.get('quantity', 1)
                
                # Get variant details - شيلنا ci.is_primary
                cursor.execute('''
                SELECT pv.id, bp.product_code, br.brand_name, pt.type_name,
                    c.color_name, pv.current_stock, ci.image_url
                FROM product_variants pv
                JOIN base_products bp ON pv.base_product_id = bp.id
                JOIN brands br ON bp.brand_id = br.id
                JOIN product_types pt ON bp.product_type_id = pt.id
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN color_images ci ON pv.id = ci.variant_id
                WHERE pv.id = ?
                LIMIT 1
                ''', (variant_id,))
                
                variant_data = cursor.fetchone()
                if variant_data:
                    detailed_items.append({
                        'variant_id': variant_data[0],
                        'product_code': variant_data[1],
                        'brand_name': variant_data[2],
                        'product_type': variant_data[3],
                        'color_name': variant_data[4],
                        'current_stock': variant_data[5],
                        'image_url': variant_data[6],
                        'quantity': quantity
                    })
            
            conn.close()
            return detailed_items
            
        except Exception as e:
            print(f"❌ Error getting session items with details: {e}")
            if 'conn' in locals():
                conn.close()
            return []


    def close_session(self, session_id, status='confirmed'):
        """Close a session with status (confirmed/cancelled)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE barcode_sessions
            SET status = ?, last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (status, session_id))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error closing session: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def cleanup_old_sessions(self, hours=24):
        """Cleanup sessions older than X hours"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if self.db_type == 'postgresql':
                cursor.execute('''
                UPDATE barcode_sessions
                SET status = 'cancelled'
                WHERE status = 'active' 
                AND created_at < NOW() - INTERVAL '%s hours'
                ''', (hours,))
            else:
                cursor.execute('''
                UPDATE barcode_sessions
                SET status = 'cancelled'
                WHERE status = 'active' 
                AND created_at < datetime('now', '-' || ? || ' hours')
                ''', (hours,))
            
            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()
            
            print(f"✅ Cleaned up {rows_affected} old sessions")
            return rows_affected
            
        except Exception as e:
            print(f"❌ Error cleaning up sessions: {e}")
            if 'conn' in locals():
                conn.close()
            return 0

    def get_variant_details_for_barcode(self, variant_id):
        """Get all variant details needed for barcode generation"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT 
                pv.id as variant_id,
                bp.product_code,
                br.brand_name,
                pt.type_name,
                c.color_name,
                c.color_code,
                pv.current_stock,
                ci.image_url,
                bp.product_size
            FROM product_variants pv
            JOIN base_products bp ON pv.base_product_id = bp.id
            JOIN brands br ON bp.brand_id = br.id
            JOIN product_types pt ON bp.product_type_id = pt.id
            JOIN colors c ON pv.color_id = c.id
            LEFT JOIN color_images ci ON pv.id = ci.variant_id
            WHERE pv.id = ?
            ''', (variant_id,))
            
            variant = cursor.fetchone()
            conn.close()
            return variant
            
        except Exception as e:
            print(f"❌ Error getting variant details: {e}")
            if 'conn' in locals():
                conn.close()
            return None


    # ========================================
    # BARCODE IMAGE MANAGEMENT
    # ========================================
    
    def get_barcodes_with_image_status(self, search='', brand_filter='', type_filter='', color_filter='', image_filter='all', limit=50, offset=0):
        """
        Get variants with barcodes and check image status
        
        Args:
            image_filter: 'all', 'with_image', 'missing_image'
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    pv.id as variant_id,
                    bp.product_code,
                    br.brand_name,
                    pt.type_name,
                    c.color_name,
                    c.color_code,
                    pv.current_stock,
                    ci.image_url,
                    bp.product_size,
                    b.barcode_number,
                    b.image_path as barcode_image,
                    b.generated_at
                FROM product_variants pv
                JOIN base_products bp ON pv.base_product_id = bp.id
                JOIN brands br ON bp.brand_id = br.id
                JOIN product_types pt ON bp.product_type_id = pt.id
                JOIN colors c ON pv.color_id = c.id
                LEFT JOIN color_images ci ON pv.id = ci.variant_id
                JOIN barcodes b ON pv.id = b.variant_id
                WHERE 1=1
            """
            params = []
            
            if search:
                query += """ AND (bp.product_code LIKE ? OR br.brand_name LIKE ? 
                            OR c.color_name LIKE ? OR b.barcode_number LIKE ?)"""
                search_param = f'%{search}%'
                params.extend([search_param, search_param, search_param, search_param])
            
            if brand_filter:
                query += " AND br.brand_name = ?"
                params.append(brand_filter)
            
            if type_filter:
                query += " AND pt.type_name = ?"
                params.append(type_filter)
            
            if color_filter:
                query += " AND c.color_name = ?"
                params.append(color_filter)
            
            query += " ORDER BY br.brand_name, bp.product_code, c.color_name"
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            variants = cursor.fetchall()
            conn.close()
            
            # Check image existence for each variant
            results = []
            for v in variants:
                image_path = v[10]  # barcode_image
                
                # Check if image file exists
                image_exists = False
                if image_path and os.path.exists(image_path):
                    # Check file size (must be > 1KB)
                    if os.path.getsize(image_path) > 1024:
                        image_exists = True
                
                # Apply image filter
                if image_filter == 'with_image' and not image_exists:
                    continue
                elif image_filter == 'missing_image' and image_exists:
                    continue
                
                results.append({
                    'variant_id': v[0],
                    'product_code': v[1],
                    'brand_name': v[2],
                    'type_name': v[3],
                    'color_name': v[4],
                    'color_code': v[5],
                    'current_stock': v[6],
                    'image_url': v[7],
                    'product_size': v[8],
                    'barcode_number': v[9],
                    'barcode_image': v[10],
                    'generated_at': v[11],
                    'image_exists': image_exists
                })
            
            return results
            
        except Exception as e:
            print(f"❌ Error getting barcodes with image status: {e}")
            if 'conn' in locals():
                conn.close()
            return []
    
    def count_barcode_image_status(self):
        """
        Count barcodes by image status
        
        Returns:
            dict: {'total': int, 'with_image': int, 'missing_image': int}
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT barcode_number, image_path
                FROM barcodes
            """)
            barcodes = cursor.fetchall()
            conn.close()
            
            total = len(barcodes)
            with_image = 0
            missing_image = 0
            
            for barcode in barcodes:
                image_path = barcode[1]
                
                # Check if image exists
                if image_path and os.path.exists(image_path):
                    if os.path.getsize(image_path) > 1024:
                        with_image += 1
                    else:
                        missing_image += 1
                else:
                    missing_image += 1
            
            return {
                'total': total,
                'with_image': with_image,
                'missing_image': missing_image
            }
            
        except Exception as e:
            print(f"❌ Error counting image status: {e}")
            return {'total': 0, 'with_image': 0, 'missing_image': 0}
    
    def update_barcode_image_path(self, variant_id, image_path):
        """Update barcode image path"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE barcodes 
                SET image_path = ?
                WHERE variant_id = ?
            """, (image_path, variant_id))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error updating barcode image path: {e}")
            if 'conn' in locals():
                conn.close()
            return False


# اختبار قاعدة البيانات المحدثة
if __name__ == "__main__":
    db = StockDatabase()
    db.add_default_data()
    print("✅ Enhanced database structure with organized image system ready!")
    print("📦 Features: Products, Colors, Brands, Types, Categories, Tags, Organized Images, Size")
