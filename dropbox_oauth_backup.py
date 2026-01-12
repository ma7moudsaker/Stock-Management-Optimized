import dropbox
import json
import sqlite3
from datetime import datetime
import os
import requests

class DropboxOAuthBackup:
    def __init__(self):
        self.app_key = os.getenv('DROPBOX_APP_KEY')
        self.app_secret = os.getenv('DROPBOX_APP_SECRET')
        self.refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN')
        self.access_token = None
        self.dbx = None
        self.max_backups = 10
                # فحص فوري للمتغيرات
        print(f"🔍 Environment Variables Check:")
        print(f"  - DROPBOX_APP_KEY: {'✅ موجود' if self.app_key else '❌ غير موجود'}")
        print(f"  - DROPBOX_APP_SECRET: {'✅ موجود' if self.app_secret else '❌ غير موجود'}")  
        print(f"  - DROPBOX_REFRESH_TOKEN: {'✅ موجود' if self.refresh_token else '❌ غير موجود'}")
        

        if self.refresh_token and self.app_key and self.app_secret:
            self.refresh_access_token()
        else:
            print("⚠️ مطلوب DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN")
    
        if not all([self.app_key, self.app_secret, self.refresh_token]):
            print("⚠️ بعض المتغيرات مفقودة - النسخ الاحتياطية ستعمل محلياً فقط")
            self.dbx = None
            return

    def refresh_access_token(self):
        """تجديد Access Token باستخدام Refresh Token مع تشخيص مفصل"""
        try:
            # فحص المتغيرات البيئية أولاً
            if not self.app_key:
                print("❌ DROPBOX_APP_KEY غير موجود")
                return False
            if not self.app_secret:
                print("❌ DROPBOX_APP_SECRET غير موجود")
                return False
            if not self.refresh_token:
                print("❌ DROPBOX_REFRESH_TOKEN غير موجود")
                return False
            
            print(f"🔄 محاولة تجديد التوكن...")
            print(f"📝 App Key: {self.app_key[:8]}..." if self.app_key else "❌ App Key فارغ")
            print(f"📝 App Secret: {self.app_secret[:8]}..." if self.app_secret else "❌ App Secret فارغ") 
            print(f"📝 Refresh Token: {self.refresh_token[:20]}..." if self.refresh_token else "❌ Refresh Token فارغ")
            
            url = 'https://api.dropboxapi.com/oauth2/token'
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
                'client_id': self.app_key,
                'client_secret': self.app_secret
            }
            
            response = requests.post(url, headers=headers, data=data, timeout=30)
            
            # تشخيص مفصل للاستجابة
            print(f"📊 Response Status: {response.status_code}")
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                self.dbx = dropbox.Dropbox(self.access_token)
                print("✅ تم تجديد Dropbox Access Token بنجاح")
                return True
            else:
                print(f"❌ فشل تجديد التوكن: {response.status_code}")
                print(f"📄 Response Text: {response.text}")
                
                # تحليل الأخطاء الشائعة
                if response.status_code == 400:
                    try:
                        error_data = response.json()
                        error_description = error_data.get('error_description', 'Unknown error')
                        print(f"🔍 تفاصيل الخطأ: {error_description}")
                        
                        if 'invalid_grant' in error_description:
                            print("💡 الـ Refresh Token منتهي الصلاحية أو غير صحيح")
                        elif 'invalid_client' in error_description:
                            print("💡 App Key أو App Secret غير صحيح")
                            
                    except:
                        print("🔍 لا يمكن تحليل تفاصيل الخطأ")
                
                return False
                
        except Exception as e:
            print(f"❌ خطأ في الاتصال بـ Dropbox API: {e}")
            return False
        
    def ensure_valid_token(self):
        """التأكد من صحة التوكن قبل أي عملية"""
        if not self.dbx:
            return self.refresh_access_token()
        
        try:
            # اختبار بسيط للتوكن
            self.dbx.users_get_current_account()
            return True
        except dropbox.exceptions.AuthError:
            print("🔄 انتهت صلاحية التوكن - تجديد...")
            return self.refresh_access_token()
        except Exception:
            return False
    
    def export_database_to_json(self):
        """تصدير قاعدة البيانات لـ JSON"""
        try:
            conn = sqlite3.connect('stock_management.db')
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            backup_data = {
                'backup_date': datetime.now().isoformat(),
                'version': '2.0',
                'source': 'dropbox_oauth_backup',
                'app_info': {
                    'name': 'Stock Management System',
                    'backup_type': 'full_database'
                },
                'tables': {}
            }
            
            important_tables = [
                'brands', 'colors', 'product_types', 'trader_categories',
                'suppliers', 'tags', 'base_products', 'product_variants',
                'color_images', 'product_tags'
            ]
            
            total_records = 0
            for table_name in important_tables:
                try:
                    cursor.execute(f"SELECT * FROM {table_name}")
                    rows = cursor.fetchall()
                    backup_data['tables'][table_name] = [dict(row) for row in rows]
                    print(f"✅ تم تصدير {len(rows)} سجل من جدول {table_name}")
                    total_records += len(rows)
                except Exception as e:
                    print(f"⚠️ تخطي جدول {table_name}: {e}")
            
            conn.close()
            print(f"📊 إجمالي السجلات المُصدرة: {total_records}")
            return backup_data
            
        except Exception as e:
            print(f"❌ خطأ في تصدير قاعدة البيانات: {e}")
            return None
    
    def create_backup(self):
        """إنشاء نسخة احتياطية مع تجديد تلقائي للتوكن"""
        print("🔄 بدء إنشاء نسخة احتياطية في Dropbox...")
        
        if not self.ensure_valid_token():
            print("❌ فشل في الاتصال بـ Dropbox - حفظ محلياً")
            return self.create_local_backup()
        
        try:
            backup_data = self.export_database_to_json()
            if not backup_data:
                return False
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'stock_backup_{timestamp}.json'
            
            json_content = json.dumps(backup_data, ensure_ascii=False, indent=2)
            content_size = len(json_content.encode('utf-8'))
            
            print(f"📦 حجم النسخة الاحتياطية: {content_size / 1024:.1f} KB")
            
            self.dbx.files_upload(
                json_content.encode('utf-8'),
                f'/{filename}',
                mode=dropbox.files.WriteMode.overwrite
            )
            
            print(f"✅ تم رفع النسخة الاحتياطية بنجاح: {filename}")
            self.cleanup_old_backups()
            return True
            
        except dropbox.exceptions.AuthError:
            print("🔄 خطأ في المصادقة - محاولة تجديد التوكن...")
            if self.refresh_access_token():
                return self.create_backup()  # إعادة المحاولة
            else:
                return self.create_local_backup()
        except Exception as e:
            print(f"❌ خطأ عام: {e}")
            return self.create_local_backup()
    
    def list_backups(self):
        """قائمة النسخ الاحتياطية مع تجديد تلقائي للتوكن"""
        if not self.ensure_valid_token():
            return []
        
        try:
            result = self.dbx.files_list_folder('')
            backups = []
            
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FileMetadata) and entry.name.startswith('stock_backup_'):
                    backups.append({
                        'name': entry.name,
                        'size': entry.size,
                        'modified': entry.server_modified.isoformat() if entry.server_modified else 'غير محدد',
                        'path': entry.path_display
                    })
            
            backups.sort(key=lambda x: x['name'], reverse=True)
            return backups
            
        except dropbox.exceptions.AuthError:
            print("🔄 خطأ في المصادقة - محاولة تجديد التوكن...")
            if self.refresh_access_token():
                return self.list_backups()  # إعادة المحاولة
            return []
        except Exception as e:
            print(f"❌ خطأ في جلب قائمة النسخ: {e}")
            return []
    
    def restore_from_backup(self, backup_name=None):
        """استرجاع البيانات من Dropbox"""
        try:
            if not self.dbx:
                print("❌ غير متصل بـ Dropbox")
                return False

            # اختيار النسخة الاحتياطية
            if not backup_name:
                backups = self.list_backups()
                if not backups:
                    print("❌ لا توجد نسخ احتياطية متوفرة")
                    return False
                backup_name = backups[0]['name']

            backup_path = f"/{backup_name}"
            print(f"🔄 استرجاع من النسخة: {backup_name}")

            # تحميل الملف من Dropbox
            _, response = self.dbx.files_download(backup_path)
            backup_data = json.loads(response.content.decode('utf-8'))

            # استرجاع البيانات
            from database import StockDatabase
            db = StockDatabase()
            
            # استرجاع كل جدول
            tables_order = [
                'brands', 'colors', 'product_types', 'trader_categories', 
                'suppliers', 'tags', 'base_products', 'product_variants', 
                'color_images', 'product_tags'
            ]

            total_restored = 0
            for table_name in tables_order:
                    if table_name in backup_data.get('tables', {}):  # ✅ صحيح
                        table_data = backup_data['tables'][table_name]  # ✅ صحيح
                    
                    # 🔧 هنا التصحيح المطلوب
                    if isinstance(table_data, list):
                        # إذا كانت البيانات قائمة، نتعامل معها مباشرة
                        restored_count = self._restore_table_data(db, table_name, table_data)
                    elif isinstance(table_data, dict):
                        # إذا كانت البيانات قاموس، نحولها لقائمة
                        rows = []
                        for row_data in table_data.values():
                            if isinstance(row_data, dict):
                                rows.append(row_data)
                        restored_count = self._restore_table_data(db, table_name, rows)
                    else:
                        print(f"⚠️ نوع بيانات غير مدعوم في جدول {table_name}: {type(table_data)}")
                        continue
                        
                    total_restored += restored_count
                    print(f"✅ تم استرجاع {restored_count} سجل من جدول {table_name}")

            print(f"🎉 تم استرجاع {total_restored} سجل بنجاح!")
            return True

        except Exception as e:
            print(f"❌ خطأ في استرجاع النسخة الاحتياطية: {e}")
            return False
        
    def _restore_table_data(self, db, table_name, rows):
        """استرجاع بيانات جدول واحد"""
        if not rows:
            return 0
            
        conn = db.get_connection()
        cursor = conn.cursor()
        restored_count = 0
        
        try:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                    
                # إنشاء استعلام INSERT
                columns = list(row.keys())
                placeholders = ', '.join(['?' for _ in columns])
                values = [row[col] for col in columns]
                
                query = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                
                try:
                    cursor.execute(query, values)
                    restored_count += 1
                except Exception as e:
                    print(f"⚠️ خطأ في إدراج سجل في {table_name}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            return restored_count
            
        except Exception as e:
            print(f"❌ خطأ في استرجاع جدول {table_name}: {e}")
            conn.close()
            return 0

    def restore_data_to_database(self, backup_data):
        """استرجاع البيانات لقاعدة البيانات مع معالجة أفضل للأخطاء"""
        try:
            conn = sqlite3.connect('stock_management.db')
            cursor = conn.cursor()
            
            # مسح البيانات الحالية
            for table_name in backup_data['tables'].keys():
                try:
                    cursor.execute(f"DELETE FROM {table_name}")
                    print(f"🗑️ تم مسح جدول {table_name}")
                except:
                    pass
            
            # استرجاع البيانات
            total_restored = 0
            for table_name, rows in backup_data['tables'].items():
                if not rows:
                    continue
                
                try:
                    # 🎯 التحقق من نوع البيانات
                    if isinstance(rows, list) and len(rows) > 0:
                        # إذا كانت البيانات في شكل list of tuples، نحولها لـ list of dicts
                        if isinstance(rows[0], (list, tuple)):
                            # نحصل على أسماء الأعمدة من الجدول
                            cursor.execute(f"PRAGMA table_info({table_name})")
                            columns_info = cursor.fetchall()
                            column_names = [col[1] for col in columns_info]
                            
                            # نحول البيانات لـ dicts
                            dict_rows = []
                            for row in rows:
                                if len(row) <= len(column_names):
                                    dict_rows.append(dict(zip(column_names, row)))
                            rows = dict_rows
                        
                        # إذا كانت البيانات دلوقتي في شكل list of dicts
                        if isinstance(rows[0], dict):
                            columns = list(rows.keys())
                            placeholders = ', '.join(['?' for _ in columns])
                            insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                            
                            for row in rows:
                                values = [row[col] for col in columns]
                                cursor.execute(insert_sql, values)
                            
                            print(f"✅ تم استرجاع {len(rows)} سجل لجدول {table_name}")
                            total_restored += len(rows)
                        else:
                            print(f"⚠️ تخطي جدول {table_name}: نوع بيانات غير مدعوم")
                    else:
                        print(f"⚠️ تخطي جدول {table_name}: بيانات فارغة أو غير صحيحة")
                        
                except Exception as e:
                    print(f"⚠️ خطأ في استرجاع جدول {table_name}: {e}")
            
            conn.commit()
            conn.close()
            
            print(f"🎉 تم استرجاع {total_restored} سجل بنجاح!")
            return True
            
        except Exception as e:
            print(f"❌ خطأ في استرجاع البيانات: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False
        
    def cleanup_old_backups(self):
        """حذف النسخ القديمة الزائدة"""
        try:
            backups = self.list_backups()
            
            if len(backups) > self.max_backups:
                old_backups = backups[self.max_backups:]
                
                for backup in old_backups:
                    self.dbx.files_delete_v2(backup['path'])
                    print(f"🗑️ حذف نسخة قديمة: {backup['name']}")
                    
        except Exception as e:
            print(f"⚠️ خطأ في تنظيف النسخ القديمة: {e}")
    
    def create_local_backup(self):
        """نسخة احتياطية محلية كـ fallback"""
        try:
            backup_data = self.export_database_to_json()
            if not backup_data:
                return False
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'local_backup_{timestamp}.json'
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ نسخة احتياطية محلية: {filename}")
            return True
            
        except Exception as e:
            print(f"❌ فشل النسخ الاحتياطي المحلي: {e}")
            return False
