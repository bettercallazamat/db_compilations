from datetime import datetime
from bson.objectid import ObjectId
from compilation_parser import CompilationParser, VideoUsageTracker


class CompilationManager:
    """Manager for handling compilation videos and their relationships"""

    def __init__(self, videos_collection, compilations_collection, user_compilations_collection=None):
        self.videos_collection = videos_collection
        self.compilations_collection = compilations_collection
        self.user_compilations_collection = user_compilations_collection

        # Fixed VideoUsageTracker initialization - check for None explicitly
        if user_compilations_collection is not None:
            self.usage_tracker = VideoUsageTracker(
                compilations_collection,
                user_compilations_collection,
                videos_collection
            )
        else:
            # Fallback: use compilations_collection for both parameters
            self.usage_tracker = VideoUsageTracker(
                compilations_collection,
                compilations_collection,
                videos_collection
            )

    def process_all_compilations(self):
        """
        Process all videos in the database to identify and extract compilations
        Returns: dict with processing results
        """
        processed = 0
        new_compilations = 0
        updated_compilations = 0
        errors = []

        # Get all videos from the database
        videos = self.videos_collection.find({})

        for video in videos:
            try:
                result = self.process_single_video(video)
                processed += 1

                if result['action'] == 'created':
                    new_compilations += 1
                elif result['action'] == 'updated':
                    updated_compilations += 1

            except Exception as e:
                errors.append({
                    'video_id': video.get('video_id', 'unknown'),
                    'error': str(e)
                })

        # Update usage statistics for all videos
        try:
            self.usage_tracker.update_video_usage_stats()
        except Exception as e:
            errors.append({
                'operation': 'update_usage_stats',
                'error': str(e)
            })

        return {
            'processed': processed,
            'new_compilations': new_compilations,
            'updated_compilations': updated_compilations,
            'errors': errors
        }

    def process_single_video(self, video_doc):
        """
        Process a single video to check if it's a compilation
        Returns: dict with processing result
        """
        compilation_data = CompilationParser.extract_compilation_data(
            video_doc)

        if not compilation_data:
            return {'action': 'skipped', 'reason': 'not_a_compilation'}

        # Check if compilation already exists
        existing = self.compilations_collection.find_one({
            'video_id': compilation_data['video_id']
        })

        if existing:
            # Update existing compilation
            self.compilations_collection.update_one(
                {'_id': existing['_id']},
                {
                    '$set': {
                        'title': compilation_data['title'],
                        'duration': compilation_data['duration'],
                        'duration_rounded': compilation_data['duration_rounded'],
                        'timestamps': compilation_data['timestamps'],
                        'view_count': compilation_data['view_count'],
                        'like_count': compilation_data['like_count'],
                        'updated_at': datetime.utcnow()
                    }
                }
            )

            # Mark original video as compilation
            self._mark_video_as_compilation(video_doc['_id'])

            return {'action': 'updated', 'compilation_id': str(existing['_id'])}

        else:
            # Create new compilation
            result = self.compilations_collection.insert_one(compilation_data)

            # Mark original video as compilation
            self._mark_video_as_compilation(video_doc['_id'])

            return {'action': 'created', 'compilation_id': str(result.inserted_id)}

    def _mark_video_as_compilation(self, video_object_id):
        """Mark a video as compilation in the videos table"""
        self.videos_collection.update_one(
            {'_id': video_object_id},
            {
                '$set': {
                    'is_compilation': True,
                    'updated_at': datetime.utcnow()
                }
            }
        )

    def get_compilation_details(self, compilation_id):
        """Get detailed information about a compilation"""
        compilation = self.compilations_collection.find_one({
            '_id': ObjectId(compilation_id)
        })

        if not compilation:
            return None

        # Get linked videos information
        linked_videos = []
        for timestamp_entry in compilation.get('timestamps', []):
            # Try to find matching videos in the database
            matching_videos = list(self.videos_collection.find({
                'title': {'$regex': timestamp_entry['title'], '$options': 'i'}
            }).limit(5))  # Limit to avoid too many results

            linked_videos.append({
                'timestamp': timestamp_entry['timestamp'],
                'title': timestamp_entry['title'],
                'matching_videos': matching_videos
            })

        return {
            'compilation': compilation,
            'linked_videos': linked_videos
        }

    def get_compilations_with_filters(self, duration_filter=None, page=1, per_page=10):
        """Get compilations with optional duration filter and pagination"""
        query = {}

        if duration_filter:
            query['duration_rounded'] = int(duration_filter)

        # Get total count
        total = self.compilations_collection.count_documents(query)

        # Get compilations with pagination
        compilations = list(self.compilations_collection.find(query)
                            .sort('created_at', -1)
                            .skip((page - 1) * per_page)
                            .limit(per_page))

        return {
            'compilations': compilations,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        }

    def get_compilation_statistics(self):
        """Get statistics about compilations"""
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'total_compilations': {'$sum': 1},
                    'total_duration': {'$sum': '$duration'},
                    'avg_duration': {'$avg': '$duration'},
                    'total_videos_in_compilations': {
                        '$sum': {'$size': '$timestamps'}
                    }
                }
            }
        ]

        stats = list(self.compilations_collection.aggregate(pipeline))

        # Get duration distribution
        duration_pipeline = [
            {
                '$group': {
                    '_id': '$duration_rounded',
                    'count': {'$sum': 1},
                    'avg_views': {'$avg': '$view_count'}
                }
            },
            {'$sort': {'_id': 1}}
        ]

        duration_stats = list(
            self.compilations_collection.aggregate(duration_pipeline))

        return {
            'overall': stats[0] if stats else {},
            'by_duration': duration_stats
        }

    def delete_compilation(self, compilation_id):
        """Delete a compilation and unmark the original video"""
        compilation = self.compilations_collection.find_one({
            '_id': ObjectId(compilation_id)
        })

        if not compilation:
            return False

        # Find and unmark the original video
        original_video = self.videos_collection.find_one({
            'video_id': compilation['video_id']
        })

        if original_video:
            self.videos_collection.update_one(
                {'_id': original_video['_id']},
                {
                    '$set': {
                        'is_compilation': False,
                        'updated_at': datetime.utcnow()
                    }
                }
            )

        # Delete the compilation
        self.compilations_collection.delete_one(
            {'_id': ObjectId(compilation_id)})

        # Update usage statistics
        self.usage_tracker.update_video_usage_stats()

        return True

    def update_video_usage_statistics(self, video_id=None):
        """Update usage statistics for videos"""
        return self.usage_tracker.update_video_usage_stats(video_id)

    def get_video_usage_report(self, video_id=None):
        """Get usage report for videos"""
        return self.usage_tracker.get_video_usage_report(video_id)
