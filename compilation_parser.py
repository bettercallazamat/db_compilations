import re
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from pymongo import UpdateOne


class CompilationParser:
    """Parser for compilation videos to extract timestamps and metadata"""

    COMPILATION_KEYWORDS = ["Mega Compilation", "+ MORE", 'compilation', 'mega', '+ more', 'collection',
                            'best of', 'minutes of', 'hours of', 'non-stop']

    @staticmethod
    def is_compilation(description: str) -> bool:
        """Check if video is a compilation based on description keywords"""
        if not description:
            return False

        return any(keyword in description for keyword in CompilationParser.COMPILATION_KEYWORDS)

    @staticmethod
    def parse_timestamps(description: str) -> List[Dict[str, str]]:
        """
        Parse timestamps from video description
        Expected format: "0:00 Title", "3:00 Another Title", etc.
        """
        if not description:
            return []

        # Pattern to match timestamps like "0:00", "3:45", "12:30", etc.
        timestamp_pattern = r'(\d{1,2}:\d{2})\s+([^\n\r]+)'
        matches = re.findall(timestamp_pattern, description)

        videos = []
        for timestamp, title in matches:
            # Clean up title (remove extra whitespace, special characters at start)
            title = title.strip()
            # Remove common prefixes like "- ", "• ", etc.
            title = re.sub(r'^[-•\-\s]+', '', title)

            if title:  # Only add if title is not empty
                videos.append({
                    'timestamp': timestamp,
                    'title': title.strip()
                })

        return videos

    @staticmethod
    def timestamp_to_seconds(timestamp: str) -> int:
        """Convert timestamp string (MM:SS or H:MM:SS) to seconds"""
        parts = timestamp.split(':')
        if len(parts) == 2:  # MM:SS
            minutes, seconds = map(int, parts)
            return minutes * 60 + seconds
        elif len(parts) == 3:  # H:MM:SS
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
        return 0

    @staticmethod
    def round_duration_to_nearest_5min(duration_seconds: int) -> int:
        """Round duration to nearest 5 minutes"""
        minutes = duration_seconds / 60
        rounded_minutes = round(minutes / 5) * 5
        return max(5, int(rounded_minutes))  # Minimum 5 minutes

    @staticmethod
    def extract_compilation_data(video_doc: dict, videos_collection=None) -> Optional[dict]:
        """Extract compilation data from video document"""
        description = video_doc.get('description', '')
        title = video_doc.get('title', '')
        # print(f" VIDEO DOC: {video_doc}")

        if not CompilationParser.is_compilation(title):
            return None

        timestamps = CompilationParser.parse_timestamps(description)

        # If videos_collection is provided, try to match video IDs
        if videos_collection is not None and timestamps:
            timestamps = CompilationParser._match_video_ids(timestamps, videos_collection)

        duration_seconds = video_doc.get('duration_seconds', 0)
        duration_rounded = CompilationParser.round_duration_to_nearest_5min(
            duration_seconds)

        compilation_data = {
            'original_video_id': video_doc.get('_id'),
            'title': video_doc.get('title', ''),
            'video_id': video_doc.get('video_id', ''),
            'duration': duration_seconds,
            'duration_rounded': duration_rounded,
            'timestamps': timestamps,
            'published_at': video_doc.get('published_at', ''),
            'view_count': video_doc.get('view_count', 0),
            'like_count': video_doc.get('like_count', 0),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }

        return compilation_data

    @staticmethod
    def _match_video_ids(timestamps: List[Dict], videos_collection) -> List[Dict]:
        """
        Match video titles to actual video IDs in the database
        """
        matched_timestamps = []

        for timestamp in timestamps:
            title = timestamp.get('title', '').strip()
            if not title:
                matched_timestamps.append(timestamp)
                continue

            # Clean the title for matching (remove common suffixes)
            clean_title = title.replace(' | D Billions Kids Songs', '').strip()
            clean_title = clean_title.replace(' | Mega Compilation | D Billions Kids Songs', '').strip()

            # Try exact title match first
            video = videos_collection.find_one({'title': title})

            # If no exact match, try cleaned title match
            if not video and clean_title != title:
                video = videos_collection.find_one({'title': clean_title})

            # If still no match, try partial title match (first 30 characters)
            if not video and len(clean_title) > 10:
                partial_title = clean_title[:30]
                video = videos_collection.find_one({
                    'title': {'$regex': re.escape(partial_title), '$options': 'i'}
                })

            # Create matched timestamp
            matched_timestamp = timestamp.copy()
            if video:
                matched_timestamp['video_id'] = video.get('video_id', '')
                # Also store the matched title for reference
                matched_timestamp['matched_title'] = video.get('title', '')
            else:
                # Mark as unmatched
                matched_timestamp['video_id'] = ''
                matched_timestamp['matched_title'] = clean_title

            matched_timestamps.append(matched_timestamp)

        return matched_timestamps


class VideoUsageTracker:
    """Track video usage statistics in compilations"""

    def __init__(self, compilations_collection, user_compilations_collection, videos_collection):
        self.compilations_collection = compilations_collection  # Auto-generated compilations
        # User-created compilations
        self.user_compilations_collection = user_compilations_collection
        self.videos_collection = videos_collection

    def update_video_usage_stats(self, video_id: str = None):
        """
        Update usage statistics for videos in compilations
        If video_id is provided, update only that video, otherwise update all
        """
        if video_id:
            self._update_single_video_stats(video_id)
        else:
            self._update_all_video_stats()

    def _update_single_video_stats(self, video_id: str):
        """Update statistics for a single video"""
        video = self.videos_collection.find_one({'video_id': video_id})
        if not video:
            return

        stats = self._calculate_video_stats(video_id)

        self.videos_collection.update_one(
            {'video_id': video_id},
            {
                '$set': {
                    'compilation_usage_stats': stats,
                    'stats_updated_at': datetime.utcnow()
                }
            }
        )

    def _update_all_video_stats(self):
        """Update statistics for all videos using bulk write operations"""
        videos = list(self.videos_collection.find({}))
        
        if not videos:
            return 0
        
        # Build bulk operations
        bulk_updates = []
        for video in videos:
            stats = self._calculate_video_stats(video.get('video_id'))
            bulk_updates.append(UpdateOne(
                {'_id': video['_id']},
                {'$set': {
                    'compilation_usage_stats': stats,
                    'stats_updated_at': datetime.utcnow()
                }}
            ))
        
        # Execute bulk write
        if bulk_updates:
            result = self.videos_collection.bulk_write(bulk_updates)
            return result.modified_count
        
        return 0

    def _calculate_video_stats(self, video_id: str) -> dict:
        """Calculate usage statistics for a video"""
        if not video_id:
            return {
                'total_inclusions': 0,
                'first_video_count': 0,
                'usage_by_duration': {},
                'first_video_by_duration': {},
                'first_video_last_used_by_duration': {},
                'auto_compilation_usage': 0,
                'user_compilation_usage': 0
            }

        one_year_ago = datetime.utcnow() - timedelta(days=365)

        # Query auto-generated compilations (from JSON imports)
        auto_compilations = list(self.compilations_collection.find({
            'timestamps.video_id': video_id,
            'created_at': {'$gte': one_year_ago}
        }))

        # Query user-created compilations (from compilation creator)
        user_compilations = list(self.user_compilations_collection.find({
            'timestamps.video_id': video_id,
            'created_at': {'$gte': one_year_ago}
        }))

        # Initialize stats structure
        stats = {
            'total_inclusions': 0,
            'first_video_count': 0,
            'usage_by_duration': {},
            'first_video_by_duration': {},
            'first_video_last_used_by_duration': {},
            'auto_compilation_usage': 0,
            'user_compilation_usage': 0,
            # Separate tracking for each type
            'auto_compilation_details': {
                'total_inclusions': 0,
                'first_video_count': 0,
                'usage_by_duration': {},
                'first_video_by_duration': {},
                'first_video_last_used_by_duration': {}
            },
            'user_compilation_details': {
                'total_inclusions': 0,
                'first_video_count': 0,
                'usage_by_duration': {},
                'first_video_by_duration': {},
                'first_video_last_used_by_duration': {}
            }
        }

        # Process auto-generated compilations
        stats['auto_compilation_usage'] = len(auto_compilations)
        for compilation in auto_compilations:
            self._process_compilation_for_stats(
                compilation, video_id, stats['auto_compilation_details'])

        # Process user-created compilations
        stats['user_compilation_usage'] = len(user_compilations)
        for compilation in user_compilations:
            self._process_compilation_for_stats(
                compilation, video_id, stats['user_compilation_details'])

        # Combine totals
        stats['total_inclusions'] = (stats['auto_compilation_details']['total_inclusions'] +
                                     stats['user_compilation_details']['total_inclusions'])

        stats['first_video_count'] = (stats['auto_compilation_details']['first_video_count'] +
                                      stats['user_compilation_details']['first_video_count'])

        # Merge duration dictionaries
        stats['usage_by_duration'] = self._merge_duration_dicts(
            stats['auto_compilation_details']['usage_by_duration'],
            stats['user_compilation_details']['usage_by_duration']
        )

        stats['first_video_by_duration'] = self._merge_duration_dicts(
            stats['auto_compilation_details']['first_video_by_duration'],
            stats['user_compilation_details']['first_video_by_duration']
        )

        # Merge last used dates (keep the most recent date)
        stats['first_video_last_used_by_duration'] = self._merge_last_used_dates(
            stats['auto_compilation_details']['first_video_last_used_by_duration'],
            stats['user_compilation_details']['first_video_last_used_by_duration']
        )

        return stats

    def _process_compilation_for_stats(self, compilation: dict, video_id: str, stats_section: dict):
        """Process a single compilation and update the provided stats section"""
        duration_rounded = compilation.get('duration_rounded', 0)
        duration_key = f"{duration_rounded}min"

        # Initialize duration keys if not present
        if duration_key not in stats_section['usage_by_duration']:
            stats_section['usage_by_duration'][duration_key] = 0
        if duration_key not in stats_section['first_video_by_duration']:
            stats_section['first_video_by_duration'][duration_key] = 0
        if duration_key not in stats_section['first_video_last_used_by_duration']:
            stats_section['first_video_last_used_by_duration'][duration_key] = None

        timestamps = compilation.get('timestamps', [])
        compilation_created_at = compilation.get('created_at')

        # Check each timestamp entry in the compilation
        for i, timestamp_entry in enumerate(timestamps):
            # Match video_id exactly
            if timestamp_entry.get('video_id') == video_id:
                stats_section['total_inclusions'] += 1
                stats_section['usage_by_duration'][duration_key] += 1

                # Check if this is the first video in the compilation
                if i == 0:
                    stats_section['first_video_count'] += 1
                    stats_section['first_video_by_duration'][duration_key] += 1
                    
                    # Track when this video was last used as first video for this duration
                    if compilation_created_at:
                        existing_date = stats_section['first_video_last_used_by_duration'][duration_key]
                        if not existing_date or compilation_created_at > existing_date:
                            stats_section['first_video_last_used_by_duration'][duration_key] = compilation_created_at

                # Break after first match to avoid double counting
                break

    def _merge_duration_dicts(self, dict1: dict, dict2: dict) -> dict:
        """Merge two duration dictionaries by adding values"""
        merged = dict1.copy()
        for key, value in dict2.items():
            merged[key] = merged.get(key, 0) + value
        return merged

    def _merge_last_used_dates(self, dict1: dict, dict2: dict) -> dict:
        """Merge two last-used-date dictionaries by keeping the most recent date"""
        merged = dict1.copy()
        for key, value in dict2.items():
            if key not in merged or (value and (not merged[key] or value > merged[key])):
                merged[key] = value
        return merged

    def get_video_usage_report(self, video_id: str = None) -> dict:
        """Get usage report for a specific video or all videos"""
        if video_id:
            video = self.videos_collection.find_one({'video_id': video_id})
            if video:
                return {
                    'video': video,
                    'stats': video.get('compilation_usage_stats', {})
                }
            return None

        # Return stats for all videos
        videos_with_stats = list(self.videos_collection.find({
            'compilation_usage_stats': {'$exists': True}
        }))

        return {
            'total_videos_tracked': len(videos_with_stats),
            'videos': videos_with_stats
        }

    def debug_compilation_contents(self, video_id: str = None):
        """Debug method to see what's in compilations for a specific video"""
        if not video_id:
            print("Please provide a video_id to debug")
            return

        print(f"DEBUG: Checking compilations for video_id: {video_id}")

        # Check auto-generated compilations
        auto_comps = list(self.compilations_collection.find({
            'timestamps.video_id': video_id
        }))

        print(f"Found {len(auto_comps)} auto-generated compilations")
        for i, comp in enumerate(auto_comps):
            print(f"  Auto-compilation {i+1}:")
            print(f"    Title: {comp.get('title', 'N/A')}")
            print(f"    Duration: {comp.get('duration_rounded', 'N/A')} min")
            print(f"    Timestamps count: {len(comp.get('timestamps', []))}")

            # Find position of video in timestamps
            timestamps = comp.get('timestamps', [])
            for j, ts in enumerate(timestamps):
                if ts.get('video_id') == video_id:
                    print(f"    Found at position {j+1} (first={j == 0})")
                    break

        # Check user-created compilations
        user_comps = list(self.user_compilations_collection.find({
            'timestamps.video_id': video_id
        }))

        print(f"Found {len(user_comps)} user-created compilations")
        for i, comp in enumerate(user_comps):
            print(f"  User-compilation {i+1}:")
            print(f"    Title: {comp.get('title', 'N/A')}")
            print(f"    Duration: {comp.get('duration_rounded', 'N/A')} min")
            print(f"    Timestamps count: {len(comp.get('timestamps', []))}")

            # Find position of video in timestamps
            timestamps = comp.get('timestamps', [])
            for j, ts in enumerate(timestamps):
                if ts.get('video_id') == video_id:
                    print(f"    Found at position {j+1} (first={j == 0})")
                    break

    def recalculate_all_stats(self):
        """Force recalculation of all video usage statistics"""
        print("Recalculating usage statistics for all videos...")

        # Get all unique video_ids that appear in any compilation
        auto_video_ids = set()
        user_video_ids = set()

        # Get video IDs from auto-generated compilations
        for comp in self.compilations_collection.find({}, {'timestamps.video_id': 1}):
            for timestamp in comp.get('timestamps', []):
                if timestamp.get('video_id'):
                    auto_video_ids.add(timestamp['video_id'])

        # Get video IDs from user compilations
        for comp in self.user_compilations_collection.find({}, {'timestamps.video_id': 1}):
            for timestamp in comp.get('timestamps', []):
                if timestamp.get('video_id'):
                    user_video_ids.add(timestamp['video_id'])

        all_used_video_ids = auto_video_ids.union(user_video_ids)

        print(
            f"Found {len(all_used_video_ids)} unique videos used in compilations")
        print(f"Auto-compilation videos: {len(auto_video_ids)}")
        print(f"User-compilation videos: {len(user_video_ids)}")

        # Update stats for all videos that appear in compilations
        updated_count = 0
        for video_id in all_used_video_ids:
            try:
                self._update_single_video_stats(video_id)
                updated_count += 1
                if updated_count % 100 == 0:  # Progress indicator
                    print(f"Updated {updated_count} videos...")
            except Exception as e:
                print(f"Error updating stats for video {video_id}: {e}")

        # Clear stats for videos that are no longer used
        cleared_count = self.videos_collection.update_many(
            {
                'video_id': {'$nin': list(all_used_video_ids)},
                'compilation_usage_stats': {'$exists': True}
            },
            {
                '$unset': {'compilation_usage_stats': 1},
                '$set': {'stats_updated_at': datetime.utcnow()}
            }
        ).modified_count

        print(f"Updated stats for {updated_count} videos")
        print(
            f"Cleared stats for {cleared_count} videos no longer in compilations")

        return {
            'updated': updated_count,
            'cleared': cleared_count,
            'total_compilation_videos': len(all_used_video_ids)
        }
