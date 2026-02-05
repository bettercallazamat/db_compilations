#!/usr/bin/env python3
"""
Migration Script: Add channel_id and channel_name fields to existing data

This script adds channel_id and channel_name fields to existing videos,
compilations, and user_compilations collections in MongoDB.

The DB Main Channel is used as the default channel for all existing data:
- channel_id: "UCi3onjs7UU2Z64i3RrVci-Q"
- channel_name: "DB Main Channel"

This script is idempotent - safe to run multiple times.
"""

from pymongo import MongoClient
from datetime import datetime
import sys


# Default channel values
DEFAULT_CHANNEL_ID = "UCi3onjs7UU2Z64i3RrVci-Q"
DEFAULT_CHANNEL_NAME = "DB Main Channel"


class ChannelMigration:
    """Handles migration of channel fields to MongoDB collections"""
    
    def __init__(self, mongo_uri="mongodb://localhost:27017/video_database"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.video_database
        self.summary = {
            'videos_updated': 0,
            'videos_skipped': 0,
            'compilations_updated': 0,
            'compilations_skipped': 0,
            'user_compilations_updated': 0,
            'user_compilations_skipped': 0,
            'channels_upserted': 0,
            'errors': []
        }
    
    def migrate_videos(self):
        """Add channel fields to videos collection"""
        print("\n📹 Migrating videos collection...")
        
        try:
            # Count documents that already have channel_id
            existing_count = self.db.videos.count_documents({'channel_id': {'$exists': True}})
            print(f"   Found {existing_count} videos with channel_id already set")
            
            # Update documents that don't have channel_id
            result = self.db.videos.update_many(
                {'channel_id': {'$exists': False}},
                {
                    '$set': {
                        'channel_id': DEFAULT_CHANNEL_ID,
                        'channel_name': DEFAULT_CHANNEL_NAME,
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            
            self.summary['videos_updated'] = result.modified_count
            self.summary['videos_skipped'] = existing_count
            print(f"   ✅ Updated {result.modified_count} videos")
            print(f"   ⏭️  Skipped {existing_count} videos (already have channel fields)")
            
        except Exception as e:
            error_msg = f"Videos migration error: {str(e)}"
            print(f"   ❌ {error_msg}")
            self.summary['errors'].append(error_msg)
    
    def migrate_compilations(self):
        """Add channel fields to compilations collection"""
        print("\n🎬 Migrating compilations collection...")
        
        try:
            # Check if compilations collection exists and has documents
            compilations_count = self.db.compilations.count_documents({})
            if compilations_count == 0:
                print("   ⏭️  No compilations found to migrate")
                return
            
            # Count documents that already have channel_id
            existing_count = self.db.compilations.count_documents({'channel_id': {'$exists': True}})
            print(f"   Found {existing_count} compilations with channel_id already set")
            
            # Update documents that don't have channel_id
            result = self.db.compilations.update_many(
                {'channel_id': {'$exists': False}},
                {
                    '$set': {
                        'channel_id': DEFAULT_CHANNEL_ID,
                        'channel_name': DEFAULT_CHANNEL_NAME,
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            
            self.summary['compilations_updated'] = result.modified_count
            self.summary['compilations_skipped'] = existing_count
            print(f"   ✅ Updated {result.modified_count} compilations")
            print(f"   ⏭️  Skipped {existing_count} compilations (already have channel fields)")
            
        except Exception as e:
            error_msg = f"Compilations migration error: {str(e)}"
            print(f"   ❌ {error_msg}")
            self.summary['errors'].append(error_msg)
    
    def migrate_user_compilations(self):
        """Add channel fields to user_compilations collection"""
        print("\n📁 Migrating user_compilations collection...")
        
        try:
            # Check if user_compilations collection exists and has documents
            user_compilations_count = self.db.user_compilations.count_documents({})
            if user_compilations_count == 0:
                print("   ⏭️  No user_compilations found to migrate")
                return
            
            # Count documents that already have channel_id
            existing_count = self.db.user_compilations.count_documents({'channel_id': {'$exists': True}})
            print(f"   Found {existing_count} user_compilations with channel_id already set")
            
            # Update documents that don't have channel_id
            result = self.db.user_compilations.update_many(
                {'channel_id': {'$exists': False}},
                {
                    '$set': {
                        'channel_id': DEFAULT_CHANNEL_ID,
                        'channel_name': DEFAULT_CHANNEL_NAME,
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            
            self.summary['user_compilations_updated'] = result.modified_count
            self.summary['user_compilations_skipped'] = existing_count
            print(f"   ✅ Updated {result.modified_count} user_compilations")
            print(f"   ⏭️  Skipped {existing_count} user_compilations (already have channel fields)")
            
        except Exception as e:
            error_msg = f"User compilations migration error: {str(e)}"
            print(f"   ❌ {error_msg}")
            self.summary['errors'].append(error_msg)
    
    def ensure_main_channel(self):
        """Ensure the DB Main Channel exists in channels collection"""
        print("\n📺 Ensuring DB Main Channel exists in channels collection...")
        
        try:
            # Check if channel already exists
            existing_channel = self.db.channels.find_one({'channel_id': DEFAULT_CHANNEL_ID})
            
            if existing_channel:
                print(f"   ⏭️  Channel '{DEFAULT_CHANNEL_NAME}' already exists")
                print(f"   Channel ID: {DEFAULT_CHANNEL_ID}")
            else:
                # Insert the main channel
                channel_doc = {
                    'channel_id': DEFAULT_CHANNEL_ID,
                    'channel_name': DEFAULT_CHANNEL_NAME,
                    'description': 'Default channel for migrated data',
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }
                self.db.channels.insert_one(channel_doc)
                self.summary['channels_upserted'] = 1
                print(f"   ✅ Created channel '{DEFAULT_CHANNEL_NAME}'")
                print(f"   Channel ID: {DEFAULT_CHANNEL_ID}")
            
        except Exception as e:
            error_msg = f"Channel creation error: {str(e)}"
            print(f"   ❌ {error_msg}")
            self.summary['errors'].append(error_msg)
    
    def run_migration(self):
        """Run the complete migration process"""
        print("=" * 60)
        print("🚀 Starting Channel Fields Migration")
        print("=" * 60)
        print(f"\n📌 Default Channel:")
        print(f"   Name: {DEFAULT_CHANNEL_NAME}")
        print(f"   ID: {DEFAULT_CHANNEL_ID}")
        
        # Ensure main channel exists
        self.ensure_main_channel()
        
        # Migrate each collection
        self.migrate_videos()
        self.migrate_compilations()
        self.migrate_user_compilations()
        
        # Print summary
        self.print_summary()
        
        return self.summary
    
    def print_summary(self):
        """Print migration summary"""
        print("\n" + "=" * 60)
        print("📊 Migration Summary")
        print("=" * 60)
        
        print(f"\n📹 Videos:")
        print(f"   Updated: {self.summary['videos_updated']}")
        print(f"   Skipped: {self.summary['videos_skipped']}")
        
        print(f"\n🎬 Compilations:")
        print(f"   Updated: {self.summary['compilations_updated']}")
        print(f"   Skipped: {self.summary['compilations_skipped']}")
        
        print(f"\n📁 User Compilations:")
        print(f"   Updated: {self.summary['user_compilations_updated']}")
        print(f"   Skipped: {self.summary['user_compilations_skipped']}")
        
        print(f"\n📺 Channels:")
        print(f"   Created: {self.summary['channels_upserted']}")
        
        if self.summary['errors']:
            print(f"\n❌ Errors ({len(self.summary['errors'])}):")
            for error in self.summary['errors']:
                print(f"   - {error}")
        else:
            print(f"\n✅ No errors occurred")
        
        print("\n" + "=" * 60)
        print("✨ Migration completed successfully!")
        print("=" * 60)
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()


def main():
    """Main entry point for the migration script"""
    print("🔌 Connecting to MongoDB...")
    
    try:
        migration = ChannelMigration()
        print("✅ Connected to MongoDB successfully\n")
        
        # Run migration
        summary = migration.run_migration()
        
        # Close connection
        migration.close()
        
        # Exit with appropriate code
        if summary['errors']:
            print(f"\n⚠️  Migration completed with {len(summary['errors'])} error(s)")
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {str(e)}")
        print("\nPlease ensure MongoDB is running and the connection string is correct.")
        sys.exit(1)


if __name__ == '__main__':
    main()
