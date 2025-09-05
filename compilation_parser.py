import re
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional


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
    def extract_compilation_data(video_doc: dict) -> Optional[dict]:
        """Extract compilation data from video document"""
        description = video_doc.get('description', '')
        title = video_doc.get('title', '')
        # print(f" VIDEO DOC: {video_doc}")

        if not CompilationParser.is_compilation(title):
            return None

        timestamps = CompilationParser.parse_timestamps(description)
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


class VideoUsageTracker:
    """Track video usage statistics in compilations"""

    def __init__(self, compilations_collection, videos_collection):
        self.compilations_collection = compilations_collection
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

        stats = self._calculate_video_stats(video['title'])

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
        """Update statistics for all videos"""
        videos = self.videos_collection.find({})

        for video in videos:
            stats = self._calculate_video_stats(video['title'])

            self.videos_collection.update_one(
                {'_id': video['_id']},
                {
                    '$set': {
                        'compilation_usage_stats': stats,
                        'stats_updated_at': datetime.utcnow()
                    }
                }
            )

    def _calculate_video_stats(self, video_title: str) -> dict:
        """Calculate usage statistics for a video based on its title"""
        # Get current date for filtering last year
        one_year_ago = datetime.utcnow() - timedelta(days=365)

        # Find compilations that include this video
        compilations = list(self.compilations_collection.find({
            'timestamps.title': {'$regex': re.escape(video_title), '$options': 'i'},
            'created_at': {'$gte': one_year_ago}
        }))

        stats = {
            'total_inclusions': 0,
            'first_video_count': 0,
            'usage_by_duration': {},
            'first_video_by_duration': {}
        }

        for compilation in compilations:
            duration_rounded = compilation['duration_rounded']
            duration_key = f"{duration_rounded}min"

            # Initialize counters for this duration if not exists
            if duration_key not in stats['usage_by_duration']:
                stats['usage_by_duration'][duration_key] = 0
            if duration_key not in stats['first_video_by_duration']:
                stats['first_video_by_duration'][duration_key] = 0

            # Check each timestamp in the compilation
            timestamps = compilation.get('timestamps', [])
            for i, timestamp_entry in enumerate(timestamps):
                if self._titles_match(video_title, timestamp_entry['title']):
                    stats['total_inclusions'] += 1
                    stats['usage_by_duration'][duration_key] += 1

                    # Check if this is the first video (index 0)
                    if i == 0:
                        stats['first_video_count'] += 1
                        stats['first_video_by_duration'][duration_key] += 1
                    break  # Don't count the same video multiple times in one compilation

        return stats

    def _titles_match(self, video_title: str, compilation_title: str) -> bool:
        """Check if two titles match (case-insensitive, fuzzy matching)"""
        # Simple fuzzy matching - you can make this more sophisticated
        video_title_clean = re.sub(r'[^\w\s]', '', video_title.lower())
        compilation_title_clean = re.sub(
            r'[^\w\s]', '', compilation_title.lower())

        # Check if titles are very similar (you can adjust the threshold)
        return (video_title_clean in compilation_title_clean or
                compilation_title_clean in video_title_clean or
                video_title_clean == compilation_title_clean)

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
