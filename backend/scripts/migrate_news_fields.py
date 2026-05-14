from backend.database import engine
from sqlalchemy import text

def migrate():
    print("Starting database migration for NewsIntelligenceItem...")
    with engine.connect() as conn:
        # source_url ekle
        try:
            conn.execute(text("ALTER TABLE news_intelligence_items ADD COLUMN source_url VARCHAR(512)"))
            conn.commit()
            print("Added source_url column.")
        except Exception as e:
            print(f"source_url column might already exist or error: {e}")

        # image_url ekle
        try:
            conn.execute(text("ALTER TABLE news_intelligence_items ADD COLUMN image_url VARCHAR(1024)"))
            conn.commit()
            print("Added image_url column.")
        except Exception as e:
            print(f"image_url column might already exist or error: {e}")
            
    print("Migration completed.")

if __name__ == "__main__":
    migrate()
