from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from datetime import datetime


class DatabaseSchema:
    """Database schema initialization and management"""

    def __init__(self, mongo_uri="mongodb://localhost:27017/video_database"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.video_database

    def create_indexes(self):
        """Create all necessary indexes for optimal performance"""

        # Videos collection indexes
        videos_indexes = [
            IndexModel([("video_id", ASCENDING)], unique=True),
            # Text search
            IndexModel([("title", "text"), ("description", "text")]),
            IndexModel([("published_at", DESCENDING)]),
            IndexModel([("is_compilation", ASCENDING)]),
            IndexModel([("actor", ASCENDING)]),
            IndexModel([("view_count", DESCENDING)]),
            IndexModel([("duration_seconds", ASCENDING)]),
            IndexModel([("created_at", DESCENDING)]),
        ]

        # Compilations collection indexes
        compilations_indexes = [
            IndexModel([("video_id", ASCENDING)], unique=True),
            IndexModel([("duration_rounded", ASCENDING)]),
            IndexModel([("created_at", DESCENDING)]),
            IndexModel([("view_count", DESCENDING)]),
            # Text search on timestamp titles
            IndexModel([("timestamps.title", "text")]),
        ]

        # Video blacklist collection indexes
        blacklist_indexes = [
            IndexModel([("video_id", ASCENDING)], unique=True),
            IndexModel([("added_date", DESCENDING)]),
            IndexModel([("added_by", ASCENDING)]),
        ]

        # Create indexes
        try:
            self.db.videos.create_indexes(videos_indexes)
            print("✅ Videos collection indexes created successfully")
        except Exception as e:
            print(f"❌ Error creating videos indexes: {e}")

        try:
            self.db.compilations.create_indexes(compilations_indexes)
            print("✅ Compilations collection indexes created successfully")
        except Exception as e:
            print(f"❌ Error creating compilations indexes: {e}")

        try:
            self.db.video_blacklist.create_indexes(blacklist_indexes)
            print("✅ Video blacklist collection indexes created successfully")
        except Exception as e:
            print(f"❌ Error creating blacklist indexes: {e}")

    def get_collection_info(self):
        """Get information about collections and their sizes"""
        collections_info = {}

        for collection_name in ['videos', 'compilations']:
            collection = self.db[collection_name]

            # Get basic stats
            stats = {
                'total_documents': collection.count_documents({}),
                'indexes': list(collection.list_indexes()),
                'size_mb': 0  # MongoDB stats would require admin privileges
            }

            # Get some sample documents
            sample_docs = list(collection.find().limit(3))
            for doc in sample_docs:
                doc['_id'] = str(doc['_id'])  # Convert ObjectId to string

            stats['sample_documents'] = sample_docs
            collections_info[collection_name] = stats

        return collections_info

    def validate_schema(self):
        """Validate that collections have the expected structure"""
        validation_results = {
            'videos': {'valid': True, 'issues': []},
            'compilations': {'valid': True, 'issues': []}
        }

        # Expected fields for videos collection
        expected_video_fields = {
            'title', 'video_id', 'published_at', 'description', 'thumbnail_url',
            'duration', 'duration_seconds', 'view_count', 'like_count', 'comment_count',
            'estimated_minutes_watched', 'average_view_duration', 'average_view_percentage',
            'retention_30s', 'actor', 'tags', 'is_compilation', 'compilation_usage_stats',
            'created_at', 'updated_at'
        }

        # Expected fields for compilations collection
        expected_compilation_fields = {
            'original_video_id', 'title', 'video_id', 'duration', 'duration_rounded',
            'timestamps', 'published_at', 'view_count', 'like_count', 'created_at', 'updated_at'
        }

        # Validate videos collection
        videos_sample = self.db.videos.find_one()
        if videos_sample:
            missing_fields = expected_video_fields - set(videos_sample.keys())
            if missing_fields:
                validation_results['videos']['valid'] = False
                validation_results['videos']['issues'].append(
                    f"Missing fields: {missing_fields}"
                )
        else:
            validation_results['videos']['issues'].append("No documents found")

        # Validate compilations collection
        compilations_sample = self.db.compilations.find_one()
        if compilations_sample:
            missing_fields = expected_compilation_fields - \
                set(compilations_sample.keys())
            if missing_fields:
                validation_results['compilations']['valid'] = False
                validation_results['compilations']['issues'].append(
                    f"Missing fields: {missing_fields}"
                )
        else:
            validation_results['compilations']['issues'].append(
                "No documents found")

        return validation_results

    def migrate_existing_videos(self):
        """Migrate existing videos to add new fields if they don't exist"""
        migration_result = {
            'videos_updated': 0,
            'compilations_processed': 0,
            'errors': []
        }

        try:
            # Add missing fields to existing videos
            result = self.db.videos.update_many(
                {'compilation_usage_stats': {'$exists': False}},
                {
                    '$set': {
                        'compilation_usage_stats': {},
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            migration_result['videos_updated'] = result.modified_count

            # Ensure all videos have the is_compilation field
            result = self.db.videos.update_many(
                {'is_compilation': {'$exists': False}},
                {
                    '$set': {
                        'is_compilation': False,
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            migration_result['videos_updated'] += result.modified_count

        except Exception as e:
            migration_result['errors'].append(f"Migration error: {str(e)}")

        return migration_result

    def cleanup_orphaned_data(self):
        """Clean up any orphaned or inconsistent data"""
        cleanup_result = {
            'orphaned_compilations': 0,
            'fixed_compilations': 0,
            'errors': []
        }

        try:
            # Find compilations whose original videos no longer exist
            compilations = list(self.db.compilations.find())

            for compilation in compilations:
                original_video = self.db.videos.find_one({
                    '_id': compilation.get('original_video_id')
                })

                if not original_video:
                    # Remove orphaned compilation
                    self.db.compilations.delete_one(
                        {'_id': compilation['_id']})
                    cleanup_result['orphaned_compilations'] += 1
                else:
                    # Ensure original video is marked as compilation
                    if not original_video.get('is_compilation', False):
                        self.db.videos.update_one(
                            {'_id': original_video['_id']},
                            {'$set': {'is_compilation': True,
                                      'updated_at': datetime.utcnow()}}
                        )
                        cleanup_result['fixed_compilations'] += 1

        except Exception as e:
            cleanup_result['errors'].append(f"Cleanup error: {str(e)}")

        return cleanup_result


def initialize_database():
    """Initialize database with proper schema and indexes"""
    print("🚀 Initializing video database schema...")

    schema_manager = DatabaseSchema()

    # Create indexes
    print("\n📊 Creating database indexes...")
    schema_manager.create_indexes()

    # Migrate existing data
    print("\n🔄 Migrating existing data...")
    migration_result = schema_manager.migrate_existing_videos()
    print(f"   Updated {migration_result['videos_updated']} videos")
    if migration_result['errors']:
        print(f"   ⚠️  Migration errors: {migration_result['errors']}")

    # Cleanup orphaned data
    print("\n🧹 Cleaning up orphaned data...")
    cleanup_result = schema_manager.cleanup_orphaned_data()
    print(
        f"   Removed {cleanup_result['orphaned_compilations']} orphaned compilations")
    print(f"   Fixed {cleanup_result['fixed_compilations']} compilation flags")
    if cleanup_result['errors']:
        print(f"   ⚠️  Cleanup errors: {cleanup_result['errors']}")

    # Validate schema
    print("\n✅ Validating schema...")
    validation_result = schema_manager.validate_schema()
    for collection, result in validation_result.items():
        if result['valid']:
            print(f"   ✅ {collection} collection: Valid")
        else:
            print(f"   ❌ {collection} collection: {result['issues']}")

    # Get collection info
    print("\n📈 Collection Information:")
    collections_info = schema_manager.get_collection_info()
    for collection, info in collections_info.items():
        print(
            f"   {collection}: {info['total_documents']} documents, {len(info['indexes'])} indexes")

    print("\n🎉 Database initialization complete!")

    return schema_manager


if __name__ == '__main__':
    initialize_database()
