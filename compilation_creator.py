from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from bson.objectid import ObjectId
import math
from enum import Enum
from compilation_parser import VideoUsageTracker


class CompilationStatus(Enum):
    """Enumeration for compilation workflow status"""
    GENERATED = "generated"    # Initial status when created
    TO_DO = "to_do"           # User can change from Generated
    READY = "ready"           # User can change from TO DO
    UPLOADED = "uploaded"     # User can change from Ready


class VideoCategory(Enum):
    """Video categories based on retention rate percentiles"""
    TOP_25_PERCENT = "top_25"
    SECOND_25_PERCENT = "second_25"
    THIRD_25_PERCENT = "third_25"
    BOTTOM_25_PERCENT = "bottom_25"


class CompilationCreator:
    """
    Advanced compilation creation system that intelligently selects videos
    based on multiple criteria including retention rates, usage frequency,
    publication dates, and duration constraints.
    """

    def __init__(self, videos_collection, compilations_collection, user_compilations_collection, blacklist_collection=None):
        self.videos_collection = videos_collection
        self.compilations_collection = compilations_collection
        self.user_compilations_collection = user_compilations_collection
        self.blacklist_collection = blacklist_collection

        # Initialize VideoUsageTracker to update stats after compilation creation
        self.usage_tracker = VideoUsageTracker(
            compilations_collection,
            user_compilations_collection,
            videos_collection
        )

        # Configuration constants for compilation creation logic
        # Maximum times a video can be used in compilations per year
        self.MAX_ANNUAL_USAGE = 10
        # Maximum duration for individual videos in compilations (in seconds)
        self.MAX_VIDEO_DURATION_SECONDS = 300  # 5 minutes
        self.MIN_VIDEO_DURATION_SECONDS = 60   # 1 minute
        self.RETENTION_WEIGHT = 0.6  # Weight factor for retention rate in scoring algorithm
        self.VIEW_COUNT_WEIGHT = 0.1  # Weight factor for view count in scoring algorithm
        self.FRESHNESS_WEIGHT = 0.1  # Weight factor for video freshness in scoring algorithm

    def get_blacklisted_video_ids(self) -> set:
        """Get set of blacklisted video IDs"""
        if self.blacklist_collection is None:
            return set()
        
        try:
            blacklisted_items = list(self.blacklist_collection.find({}, {'video_id': 1}))
            return {item['video_id'] for item in blacklisted_items}
        except Exception as e:
            print(f"Error fetching blacklist: {e}")
            return set()

    def get_available_durations(self) -> List[int]:
        """
        Retrieve all available compilation durations based on existing compilations
        in the database, rounded to nearest 5-minute intervals.
        
        Returns:
            List[int]: Sorted list of available duration options (5, 10, 15, 20, etc.)
        """
        pipeline = [
            {'$match': {'duration_rounded': {'$exists': True}}},
            {'$group': {'_id': '$duration_rounded'}},
            {'$sort': {'_id': 1}}
        ]

        existing_durations = list(
            self.compilations_collection.aggregate(pipeline))
        durations = [dur['_id'] for dur in existing_durations]

        # Add common duration options if not present
        standard_durations = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
        for duration in standard_durations:
            if duration not in durations:
                durations.append(duration)

        return sorted(durations)

    def categorize_videos_by_retention(self, videos: List[Dict], from_date: Optional[str] = None,
                                      to_date: Optional[str] = None, tags: Optional[List[str]] = None) -> Dict[VideoCategory, List[Dict]]:
        """
        Categorize videos into retention rate percentiles with sophisticated filtering
        and sorting algorithms to ensure optimal video selection.

        Args:
            videos: List of video documents from database
            from_date: Optional start date filter in 'YYYY-MM-DD' format
            to_date: Optional end date filter in 'YYYY-MM-DD' format
            tags: Optional list of tags to filter videos by (videos must have at least one tag)

        Returns:
            Dict mapping video categories to sorted lists of videos
        """
        
        # Get blacklisted video IDs
        blacklisted_ids = self.get_blacklisted_video_ids()

        # Add these counters at the beginning
        filter_stats = {
            'total_videos': len(videos),
            'filtered_compilation_flag': 0,
            'filtered_title_keywords': 0,
            'filtered_exists_in_compilations': 0,
            'filtered_duration': 0,
            'filtered_date': 0,
            'filtered_usage_limit': 0,
            'filtered_missing_metrics': 0,
            'filtered_blacklist': 0,
            'passed_all_filters': 0
        }

        filtered_videos = []
        current_date = datetime.utcnow()

        for video in videos:
            # Skip blacklisted videos
            video_id = video.get('video_id', '')
            if video_id in blacklisted_ids:
                filter_stats['filtered_blacklist'] += 1
                continue

            # Skip compilation videos - multiple checks for robustness
            if video.get('is_compilation', False):
                filter_stats['filtered_compilation_flag'] += 1
                continue

            # Additional compilation detection methods
            # video_title = video.get('title', '').lower()
            # if any(keyword in video_title for keyword in ['compilation', 'best of', 'highlights', 'collection', "more", 'songs']):
            #     filter_stats['filtered_title_keywords'] += 1
            #     continue

            # Check if video_id exists in compilations collection (extra safety)
            if self.compilations_collection.find_one({'video_ids': video.get('video_id')}):
                filter_stats['filtered_exists_in_compilations'] += 1
                continue

            # Check video duration constraints (1-5 minutes)
            video_duration = video.get('duration_seconds', 0)
            if (video_duration > self.MAX_VIDEO_DURATION_SECONDS or
                video_duration < self.MIN_VIDEO_DURATION_SECONDS or
                    video_duration == 0):
                filter_stats['filtered_duration'] += 1
                continue

            # Apply tag filter if specified
            if tags and tags:
                video_tags = video.get('tags', [])
                # Check if video has at least one of the selected tags
                has_selected_tag = any(tag in video_tags for tag in tags)
                if not has_selected_tag:
                    continue

            # Apply date range filter if specified
            if from_date or to_date:
                try:
                    video_published = video.get('published_at', '')
                    if not video_published:
                        filter_stats['filtered_date'] += 1
                        continue

                    # Handle different date formats in database
                    # Database may have: "2025-07-05T11:53:31Z" or "2025-07-05"
                    if 'T' in video_published:
                        # Full ISO 8601 format: extract date part
                        video_date = datetime.fromisoformat(video_published.replace('Z', '+00:00')).date()
                    else:
                        # Just date format
                        video_date = datetime.strptime(video_published, '%Y-%m-%d').date()

                    # Handle from_date (start of range)
                    if from_date:
                        from_date_obj = datetime.strptime(from_date, '%Y-%m-%d').date()
                        if video_date < from_date_obj:
                            filter_stats['filtered_date'] += 1
                            continue

                    # Handle to_date (end of range)
                    if to_date:
                        to_date_obj = datetime.strptime(to_date, '%Y-%m-%d').date()
                        # Include the end date by going to end of day
                        if video_date > to_date_obj:
                            filter_stats['filtered_date'] += 1
                            continue

                except (ValueError, TypeError) as e:
                    # If date parsing fails, include the video rather than exclude it
                    # This prevents videos from being lost due to date format issues
                    continue

            # Check annual usage limit
            usage_stats = video.get('compilation_usage_stats', {})
            total_usage = usage_stats.get('total_inclusions', 0)
            if total_usage >= self.MAX_ANNUAL_USAGE:
                filter_stats['filtered_usage_limit'] += 1
                continue

            # Ensure video has required metrics
            if not all(key in video for key in ['retention_30s', 'view_count']):
                filter_stats['filtered_missing_metrics'] += 1
                continue

            filter_stats['passed_all_filters'] += 1
            filtered_videos.append(video)

        # Print the debugging info
        print("VIDEO FILTERING STATS:")
        for key, value in filter_stats.items():
            print(f"  {key}: {value}")

        if not filtered_videos:
            return {category: [] for category in VideoCategory}

        # Sort videos by comprehensive scoring algorithm
        def calculate_video_score(video: Dict) -> float:
            """
            Calculate composite score for video selection using multiple weighted factors
            """
            retention_rate = video.get('retention_30s', 0) / 100.0

            view_count = video.get('view_count', 0)

            # Normalize view count (log scale to prevent outliers from dominating)
            max_views = max(v.get('view_count', 1) for v in filtered_videos)
            normalized_views = math.log(
                view_count + 1) / math.log(max_views + 1)

            # Calculate freshness factor (newer videos get slight boost)
            try:
                video_date = datetime.strptime(
                    video.get('published_at', ''), '%Y-%m-%d')
                days_old = (current_date - video_date).days
                # Decreases linearly over a year
                freshness_factor = max(0, 1 - (days_old / 365))
            except (ValueError, TypeError):
                freshness_factor = 0

            # Composite score calculation
            score = (
                self.RETENTION_WEIGHT * retention_rate +
                self.VIEW_COUNT_WEIGHT * normalized_views +
                self.FRESHNESS_WEIGHT * freshness_factor
            )

            return score

        # Sort videos by calculated scores
        sorted_videos = sorted(
            filtered_videos, key=calculate_video_score, reverse=True)

        # Divide into quartiles (25% each)
        total_count = len(sorted_videos)
        quartile_size = total_count // 4

        categories = {
            VideoCategory.TOP_25_PERCENT: sorted_videos[:quartile_size],
            VideoCategory.SECOND_25_PERCENT: sorted_videos[quartile_size:2*quartile_size],
            VideoCategory.THIRD_25_PERCENT: sorted_videos[2*quartile_size:3*quartile_size],
            VideoCategory.BOTTOM_25_PERCENT: sorted_videos[3*quartile_size:]
        }

        return categories

    def categorize_videos_for_preview(self, videos: List[Dict], from_date: Optional[str] = None,
                                      to_date: Optional[str] = None, tags: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Categorize videos by retention_30s for preview display only.
        Returns counts for High (>75%), Good (>50%), Fair (>25%), Low (<25%)

        Args:
            videos: List of video documents from database
            from_date: Optional start date filter in 'YYYY-MM-DD' format
            to_date: Optional end date filter in 'YYYY-MM-DD' format

        Returns:
            Dict with counts: {'high': count, 'good': count, 'fair': count, 'low': count}
        """
        filtered_videos = []
        current_date = datetime.utcnow()

        for video in videos:
            # Skip compilation videos
            if video.get('is_compilation', False):
                continue

            # Check if video_id exists in compilations collection
            if self.compilations_collection.find_one({'video_ids': video.get('video_id')}):
                continue

            # Check video duration constraints (1-5 minutes)
            video_duration = video.get('duration_seconds', 0)
            if (video_duration > self.MAX_VIDEO_DURATION_SECONDS or
                video_duration < self.MIN_VIDEO_DURATION_SECONDS or
                    video_duration == 0):
                continue

            # Apply tag filter if specified
            if tags and tags:
                video_tags = video.get('tags', [])
                # Check if video has at least one of the selected tags
                has_selected_tag = any(tag in video_tags for tag in tags)
                if not has_selected_tag:
                    continue

            # Apply date range filter if specified
            if from_date or to_date:
                try:
                    video_published = video.get('published_at', '')
                    if not video_published:
                        continue

                    # Handle different date formats in database
                    if 'T' in video_published:
                        video_date = datetime.fromisoformat(video_published.replace('Z', '+00:00')).date()
                    else:
                        video_date = datetime.strptime(video_published, '%Y-%m-%d').date()

                    # Handle from_date (start of range)
                    if from_date:
                        from_date_obj = datetime.strptime(from_date, '%Y-%m-%d').date()
                        if video_date < from_date_obj:
                            continue

                    # Handle to_date (end of range)
                    if to_date:
                        to_date_obj = datetime.strptime(to_date, '%Y-%m-%d').date()
                        if video_date > to_date_obj:
                            continue

                except (ValueError, TypeError):
                    continue

            # Check annual usage limit
            usage_stats = video.get('compilation_usage_stats', {})
            total_usage = usage_stats.get('total_inclusions', 0)
            if total_usage >= self.MAX_ANNUAL_USAGE:
                continue

            # Ensure video has required metrics
            if not all(key in video for key in ['retention_30s', 'view_count']):
                continue

            filtered_videos.append(video)

        # Categorize by fixed retention thresholds for preview
        counts = {'high': 0, 'good': 0, 'fair': 0, 'low': 0}

        for video in filtered_videos:
            retention_rate = video.get('retention_30s', 0)
            if retention_rate > 75:
                counts['high'] += 1
            elif retention_rate > 50:
                counts['good'] += 1
            elif retention_rate > 25:
                counts['fair'] += 1
            else:
                counts['low'] += 1

        return counts

    def select_first_video(self, duration_rounded: int, categorized_videos: Dict[VideoCategory, List[Dict]]) -> Optional[Dict]:
        """
        Select the optimal first video for a compilation using advanced selection criteria.
        The first video is crucial as it determines viewer engagement and retention.
        
        Enforces specific constraints:
        1. If a video was used as first video in X-minute compilation, it cannot be used 
           in X-minute compilations for next 365 days
        2. Videos can be used in compilations with different durations
        3. Each video can only be used once as first video per duration per year
        
        Args:
            duration_rounded: Target compilation duration
            categorized_videos: Videos categorized by retention percentiles
            
        Returns:
            Selected video document or None if no suitable video found
        """
        # Priority order for video selection
        priority_categories = [
            VideoCategory.TOP_25_PERCENT,
            VideoCategory.SECOND_25_PERCENT,
            VideoCategory.THIRD_25_PERCENT,
            VideoCategory.BOTTOM_25_PERCENT
        ]

        duration_key = f"{duration_rounded}min"

        for category in priority_categories:
            candidates = categorized_videos.get(category, [])

            if not candidates:
                continue

            # Check date constraint and ensure videos are within duration limit
            cutoff_date = datetime(2024, 9, 11)  # Updated date constraint
            valid_candidates = []

            for v in candidates:
                try:
                    published_at = v.get('published_at', '')
                    video_date = datetime.fromisoformat(
                        published_at.replace('Z', '+00:00'))
                    video_date = video_date.replace(tzinfo=None)

                    if video_date > cutoff_date:
                        # Check duration constraint
                        duration = v.get('duration_seconds', 0)
                        if self.MIN_VIDEO_DURATION_SECONDS <= duration <= self.MAX_VIDEO_DURATION_SECONDS:
                            valid_candidates.append(v)
                except (ValueError, TypeError):
                    continue

            if not valid_candidates:
                continue

            # Apply 365-day constraint logic for first video selection
            eligible_videos = self._filter_videos_by_365_day_constraint(
                valid_candidates, duration_key)

            if not eligible_videos:
                continue

            # If there are eligible videos, randomly select from them
            import random
            return random.choice(eligible_videos)

        # If we get here, no suitable video was found in any category
        return None

    def _filter_videos_by_365_day_constraint(self, candidates: List[Dict], duration_key: str) -> List[Dict]:
        """
        Filter videos based on 365-day usage constraint for first video selection.
        
        Constraint Logic:
        1. If video was used as first video in X-minute compilation, 
           it cannot be used in X-minute compilations for next 365 days
        2. Videos can be used in compilations with different durations
        3. Only include videos that haven't been used as first video for this duration in last 365 days
        
        Args:
            candidates: List of candidate videos
            duration_key: Duration key (e.g., "25min", "30min")
            
        Returns:
            Filtered list of eligible videos
        """
        one_year_ago = datetime.utcnow() - timedelta(days=365)
        eligible_videos = []
        
        for video in candidates:
            usage_stats = video.get('compilation_usage_stats', {})
            first_video_by_duration = usage_stats.get('first_video_by_duration', {})
            first_video_last_used_by_duration = usage_stats.get('first_video_last_used_by_duration', {})
            
            usage_count = first_video_by_duration.get(duration_key, 0)
            last_used_date = first_video_last_used_by_duration.get(duration_key)
            
            # Check if video was used as first video within last 365 days
            was_used_recently = False
            if last_used_date and isinstance(last_used_date, datetime):
                if last_used_date > one_year_ago:
                    was_used_recently = True
            
            # Only include videos that haven't been used as first video in last 365 days
            if not was_used_recently:
                eligible_videos.append(video)
        
        return eligible_videos

    def calculate_compilation_duration_seconds(self, selected_videos: List[Dict]) -> int:
        """
        Calculate the total duration in seconds for selected videos,
        accounting for potential transitions and intro/outro segments.
        
        Args:
            selected_videos: List of selected video documents
            
        Returns:
            Total duration in seconds
        """
        total_duration = 0
        transition_time = 2  # 2 seconds transition between videos

        for video in selected_videos:
            video_duration = video.get('duration_seconds', 0)
            total_duration += video_duration

        # Add transition times (n-1 transitions for n videos)
        if len(selected_videos) > 1:
            total_duration += (len(selected_videos) - 1) * transition_time

        return total_duration

    def select_additional_videos(self, target_duration_minutes: int, first_video: Dict,
                                 categorized_videos: Dict[VideoCategory, List[Dict]]) -> List[Dict]:
        """
        Intelligently select additional videos to fill the compilation duration
        using a sophisticated algorithm that balances content variety and engagement.
        
        Args:
            target_duration_minutes: Target compilation duration in minutes
            first_video: Already selected first video
            categorized_videos: Videos categorized by retention percentiles
            
        Returns:
            List of selected videos including the first video
        """
        selected_videos = [first_video]
        target_duration_seconds = target_duration_minutes * 60
        current_duration = first_video.get('duration_seconds', 0)

        # Create a pool of remaining candidates from all categories
        all_candidates = []
        used_video_ids = {first_video['video_id']}

        # Updated category weights to prioritize lower-quality videos
        category_weights = {
            VideoCategory.TOP_25_PERCENT: 0.10,       # 10% from top tier
            VideoCategory.SECOND_25_PERCENT: 0.15,   # 15% from second tier
            VideoCategory.THIRD_25_PERCENT: 0.30,   # 30% from third tier
            VideoCategory.BOTTOM_25_PERCENT: 0.45   # 45% from bottom tier
        }

        # Build weighted candidate pool
        for category, weight in category_weights.items():
            category_videos = categorized_videos.get(category, [])
            # Filter out already used videos and apply duration constraint
            available_videos = [
                v for v in category_videos
                if v['video_id'] not in used_video_ids and
                v.get('duration_seconds', 0) <= self.MAX_VIDEO_DURATION_SECONDS
            ]

            # Add videos with category weight information
            for video in available_videos:
                video_copy = video.copy()
                video_copy['_selection_weight'] = weight
                all_candidates.append(video_copy)

        # Sort candidates by a combination of duration fit and quality score
        def selection_score(video: Dict) -> float:
            """Calculate selection score for remaining video selection"""
            remaining_duration = target_duration_seconds - current_duration
            video_duration = video.get('duration_seconds', 0)

            # Duration fit score (prefer videos that fit well in remaining time)
            if video_duration <= remaining_duration:
                duration_fit = 1.0 - \
                    abs(remaining_duration - video_duration) / \
                    remaining_duration
            else:
                # Penalty for videos that are too long
                duration_fit = max(
                    0, 1.0 - (video_duration - remaining_duration) / remaining_duration)

            # Quality score based on retention and category weight
            # Modified to give more weight to the category weight for lower-tier videos
            retention_score = video.get('retention_30s', 0) / 100.0
            category_weight = video.get('_selection_weight', 0.1)

            # Boost lower-tier videos by giving more importance to category weight
            quality_score = retention_score * category_weight

            # Combined score - you might want to adjust these ratios
            return 0.6 * duration_fit + 0.4 * quality_score

        # Continue selecting videos until target duration is approximately met
        while current_duration < target_duration_seconds * 0.95:  # 95% of target duration
            if not all_candidates:
                break

            # Re-sort candidates based on current state
            available_candidates = [
                v for v in all_candidates
                if v['video_id'] not in used_video_ids and
                v.get('duration_seconds', 0) <= min(
                    self.MAX_VIDEO_DURATION_SECONDS,
                    (target_duration_seconds - current_duration) + 30  # 30s buffer
                )
            ]

            if not available_candidates:
                break

            # Group candidates by usage count to prioritize less-used videos
            usage_groups = {}
            for video in available_candidates:
                usage_stats = video.get('compilation_usage_stats', {})
                total_usage = usage_stats.get('total_inclusions', 0)
                
                if total_usage not in usage_groups:
                    usage_groups[total_usage] = []
                usage_groups[total_usage].append(video)

            # Sort usage groups by total usage (ascending - less used first)
            sorted_usage_groups = sorted(usage_groups.items(), key=lambda x: x[0])
            
            # Get videos with minimum usage count
            min_usage = sorted_usage_groups[0][0]
            min_usage_videos = sorted_usage_groups[0][1]
            
            # Randomly select from the least used videos
            import random
            selected_video = random.choice(min_usage_videos)

            selected_videos.append(selected_video)
            used_video_ids.add(selected_video['video_id'])
            current_duration += selected_video.get('duration_seconds', 0)

            # Remove selected video from future consideration
            all_candidates = [
                v for v in all_candidates if v['video_id'] != selected_video['video_id']]

        # Sort the final selection from highest quality to lowest quality
        # Keep the first video in its position, then sort the rest
        if len(selected_videos) > 1:
            first_video = selected_videos[0]  # Keep first video at the beginning
            remaining_videos = selected_videos[1:]

            # Sort remaining videos by retention rate (highest to lowest)
            remaining_videos.sort(key=lambda v: v.get(
                'retention_30s', 0), reverse=True)

            # Reconstruct the list with first video first, then sorted videos
            selected_videos = [first_video] + remaining_videos

        return selected_videos

    def create_live_compilation_videos(self, target_duration_minutes: int, first_video: Dict,
                                      categorized_videos: Dict[VideoCategory, List[Dict]]) -> List[Dict]:
        """
        Create videos for live compilation with specific retention-based pattern:
        1st video: retention > 70%
        2nd video: retention > 40%
        3rd & 4th videos: retention < 40%
        This cycle repeats until target duration reached

        Args:
            target_duration_minutes: Target compilation duration
            first_video: The first video (should have retention > 70%)
            categorized_videos: Videos categorized by retention percentiles

        Returns:
            List of selected videos in the live compilation pattern
        """
        # Get all available videos and filter by retention thresholds
        all_videos = []
        for category_videos in categorized_videos.values():
            all_videos.extend(category_videos)

        # Filter videos by retention thresholds
        high_retention = [v for v in all_videos if v.get('retention_30s', 0) > 70]  # >70%
        medium_retention = [v for v in all_videos if 40 < v.get('retention_30s', 0) <= 70]  # >40% and <=70%
        low_retention = [v for v in all_videos if v.get('retention_30s', 0) < 40]  # <40%

        # Remove first video from the pool to avoid duplication
        first_video_id = first_video.get('video_id')
        high_retention = [v for v in high_retention if v.get('video_id') != first_video_id]

        # Target duration in seconds
        target_duration_seconds = target_duration_minutes * 60
        current_duration = first_video.get('duration_seconds', 0)

        # Start with first video (must have retention > 70%)
        if first_video.get('retention_30s', 0) <= 70:
            # Find a replacement first video with retention > 70%
            replacement = None
            for video in high_retention:
                if video.get('video_id') not in {first_video_id}:
                    replacement = video
                    break
            if replacement:
                first_video = replacement
                high_retention = [v for v in high_retention if v.get('video_id') != replacement.get('video_id')]

        selected_videos = [first_video]
        used_video_ids = {first_video.get('video_id')}

        # Live compilation pattern: 1st (>70%) → 2nd (>40%) → 3rd (<40%) → 4th (<40%), repeating
        pattern = [
            ('high', high_retention),      # >70%
            ('medium', medium_retention),  # >40%
            ('low', low_retention),        # <40%
            ('low', low_retention)         # <40%
        ]

        current_pattern_index = 0  # Start after first (which is high retention)

        while current_duration < target_duration_seconds:
            pattern_type, video_pool = pattern[current_pattern_index % len(pattern)]

            # Find a video from the pool that hasn't been used, prioritizing less-used videos
            valid_videos = []
            for video in video_pool:
                if video.get('video_id') not in used_video_ids:
                    video_duration = video.get('duration_seconds', 0)
                    # Check if adding this video would exceed the target by more than 60 seconds
                    if current_duration + video_duration <= target_duration_seconds + 60:
                        valid_videos.append(video)

            if valid_videos:
                # Group by usage count to prioritize less-used videos
                usage_groups = {}
                for video in valid_videos:
                    usage_stats = video.get('compilation_usage_stats', {})
                    total_usage = usage_stats.get('total_inclusions', 0)
                    
                    if total_usage not in usage_groups:
                        usage_groups[total_usage] = []
                    usage_groups[total_usage].append(video)

                # Sort usage groups by total usage (ascending - less used first)
                sorted_usage_groups = sorted(usage_groups.items(), key=lambda x: x[0])
                
                # Get videos with minimum usage count
                min_usage = sorted_usage_groups[0][0]
                min_usage_videos = sorted_usage_groups[0][1]
                
                # Randomly select from the least used videos
                import random
                selected_video = random.choice(min_usage_videos)
            else:
                # If we can't find a suitable video in the current pattern, try to find any unused video
                all_remaining = []
                for pool in [high_retention, medium_retention, low_retention]:
                    all_remaining.extend([v for v in pool if v.get('video_id') not in used_video_ids])

                if not all_remaining:
                    break  # No more videos available

                # Group remaining videos by usage count and pick from least used
                remaining_usage_groups = {}
                for video in all_remaining:
                    usage_stats = video.get('compilation_usage_stats', {})
                    total_usage = usage_stats.get('total_inclusions', 0)
                    
                    if total_usage not in remaining_usage_groups:
                        remaining_usage_groups[total_usage] = []
                    remaining_usage_groups[total_usage].append(video)

                # Get the least used videos
                sorted_remaining_groups = sorted(remaining_usage_groups.items(), key=lambda x: x[0])
                min_remaining_usage = sorted_remaining_groups[0][0]
                min_remaining_videos = sorted_remaining_groups[0][1]
                
                # Sort by duration and pick the shortest that fits
                min_remaining_videos.sort(key=lambda v: v.get('duration_seconds', float('inf')))
                selected_video = None
                for video in min_remaining_videos:
                    if current_duration + video.get('duration_seconds', 0) <= target_duration_seconds + 60:
                        selected_video = video
                        break

                if not selected_video:
                    break  # No video fits the duration

            # Add the selected video
            selected_videos.append(selected_video)
            used_video_ids.add(selected_video.get('video_id'))
            current_duration += selected_video.get('duration_seconds', 0) + 2  # +2 for transitions

            current_pattern_index += 1

        return selected_videos

    def create_compilation(self, duration_minutes: int, from_date: Optional[str] = None,
                           to_date: Optional[str] = None, tags: Optional[List[str]] = None,
                           title_prefix: str = "Auto-Generated",
                           user_id: str = "system", compilation_type: str = "default", 
                           channel_id: Optional[str] = None, channel_name: Optional[str] = None, 
                           return_compilation_doc: bool = False) -> Dict:
        """
        Create a new compilation with sophisticated video selection and metadata generation.
        This is the main entry point for compilation creation.

        Args:
            duration_minutes: Target duration in minutes (will be rounded to nearest 5)
            from_date: Optional start date filter in 'YYYY-MM-DD' format
            to_date: Optional end date filter in 'YYYY-MM-DD' format
            tags: Optional list of tags to filter videos by (videos must have at least one tag)
            title_prefix: Prefix for the generated compilation title
            user_id: ID of the user creating the compilation
            compilation_type: Type of compilation ('default' or 'live')
            return_compilation_doc: If True, return the full compilation document instead of summary

        Returns:
            Dictionary containing creation results and compilation metadata
        """
        # Round duration to nearest 5 minutes
        duration_rounded = round(duration_minutes / 5) * 5
        if duration_rounded < 5:
            duration_rounded = 5

        # Get videos for categorization - filter by channel_id if provided
        query = {}
        if channel_id:
            query['channel_id'] = channel_id
            print(f"🔍 DEBUG: Filtering videos by channel_id: {channel_id}")
        else:
            print(f"⚠️  WARNING: No channel_id provided, fetching ALL videos!")
        
        all_videos = list(self.videos_collection.find(query))
        if all_videos:
            print(f"📊 DEBUG: Found {len(all_videos)} videos for compilation creation")
        else:
            print(f"❌ DEBUG: No videos found with channel_id filter")

        if not all_videos:
            return {
                'success': False,
                'error': 'No videos available in database',
                'compilation_id': None
            }

        # Categorize videos by retention rate with date and tag filtering
        categorized_videos = self.categorize_videos_by_retention(
            all_videos, from_date, to_date, tags)

        # Check if any videos are available after filtering
        total_available = sum(len(videos)
                              for videos in categorized_videos.values())
        if total_available == 0:
            return {
                'success': False,
                'error': 'No videos available after applying filters (including 10-minute duration limit)',
                'compilation_id': None
            }

        # Select the first video (highest priority)
        first_video = self.select_first_video(
            duration_rounded, categorized_videos)
        if not first_video:
            return {
                'success': False,
                'error': 'No suitable first video found (under 10 minutes)',
                'compilation_id': None
            }

        # Check if this is a live compilation
        if compilation_type == 'live':
            # Use live compilation selection algorithm
            selected_videos = self.create_live_compilation_videos(
                duration_rounded, first_video, categorized_videos)
        else:
            # Select additional videos to fill the compilation
            selected_videos = self.select_additional_videos(
                duration_rounded, first_video, categorized_videos)

        if len(selected_videos) < 2:
            return {
                'success': False,
                'error': 'Insufficient videos to create meaningful compilation (under 10 minutes each)',
                'compilation_id': None
            }

        # Calculate actual total duration
        actual_duration_seconds = self.calculate_compilation_duration_seconds(
            selected_videos)

        # Ensure first video in compilation cannot have actor status checked
        first_video_id = selected_videos[0]['video_id']
        first_video = self.videos_collection.find_one({'video_id': first_video_id})
        
        if first_video and first_video.get('actor', False):
            # Set actor status to False for first video in compilation
            self.videos_collection.update_one(
                {'video_id': first_video_id},
                {
                    '$set': {
                        'actor': False,
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            print(f"⚠️  Set actor status to False for first video in compilation: {first_video.get('title', 'Unknown')}")

        # Generate compilation metadata
        compilation_doc = self._generate_compilation_document(
            selected_videos, duration_rounded, actual_duration_seconds,
            title_prefix, user_id, from_date, compilation_type,
            channel_id, channel_name
        )

        # Save to database
        try:
            result = self.user_compilations_collection.insert_one(
                compilation_doc)
            compilation_id = str(result.inserted_id)

            # Update usage statistics for videos after compilation creation
            # This ensures both default and live compilations update usage stats properly
            try:
                self.usage_tracker.update_video_usage_stats()
            except Exception as stats_error:
                # Log stats update error but don't fail the compilation creation
                print(f"Warning: Failed to update usage stats after compilation creation: {stats_error}")

            # If return_compilation_doc is True, return the full document
            if return_compilation_doc:
                compilation_doc['_id'] = result.inserted_id
                return {
                    'success': True,
                    'compilation_doc': compilation_doc,
                    'compilation_id': compilation_id
                }

            return {
                'success': True,
                'compilation_id': compilation_id,
                'selected_videos_count': len(selected_videos),
                'target_duration_minutes': duration_rounded,
                'actual_duration_seconds': actual_duration_seconds,
                'actual_duration_minutes': round(actual_duration_seconds / 60, 1),
                'video_categories_used': self._analyze_category_usage(selected_videos, categorized_videos),
                'max_individual_video_duration': f"{self.MAX_VIDEO_DURATION_SECONDS // 60} minutes"
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'Database error: {str(e)}',
                'compilation_id': None
            }

    def _generate_compilation_document(self, selected_videos: List[Dict], duration_rounded: int,
                                       actual_duration_seconds: int, title_prefix: str,
                                       user_id: str, from_date: Optional[str], compilation_type: str = "default",
                                       channel_id: Optional[str] = None, channel_name: Optional[str] = None) -> Dict:
        """
        Generate comprehensive compilation document with all necessary metadata
        """
        # Create timestamps for the compilation
        timestamps = []
        current_time_seconds = 0

        for video in selected_videos:
            # Format timestamp as MM:SS or H:MM:SS
            minutes = current_time_seconds // 60
            seconds = current_time_seconds % 60

            if minutes >= 60:
                hours = minutes // 60
                minutes = minutes % 60
                timestamp = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                timestamp = f"{minutes}:{seconds:02d}"

            timestamps.append({
                'timestamp': timestamp,
                'title': video['title'],
                'video_id': video['video_id'],
                'original_duration': video.get('duration_seconds', 0),
                'original_duration_formatted': f"{video.get('duration_seconds', 0) // 60}:{video.get('duration_seconds', 0) % 60:02d}",
                'retention_rate': video.get('retention_30s', 0)
            })

            # 2s transition
            current_time_seconds += video.get('duration_seconds', 0) + 2

        # Generate intelligent title
        compilation_title = self._generate_compilation_title(
            selected_videos, title_prefix, duration_rounded, compilation_type)

        # Calculate compilation metrics
        avg_retention = sum(v.get('retention_30s', 0)
                            for v in selected_videos) / len(selected_videos)
        total_original_views = sum(v.get('view_count', 0)
                                   for v in selected_videos)

        compilation_doc = {
            'title': compilation_title,
            'compilation_type': compilation_type,
            'duration_rounded': duration_rounded,
            'actual_duration_seconds': actual_duration_seconds,
            'status': CompilationStatus.GENERATED.value,
            'done_by': 'TBD',  # Default value for new compilations
            'created_by': user_id,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'from_date_filter': from_date,
            'video_count': len(selected_videos),
            'timestamps': timestamps,
            # Channel information
            'channel_id': channel_id or '',
            'channel_name': channel_name or '',
            'metadata': {
                'retention_30s': round(avg_retention, 2),
                'total_original_views': total_original_views,
                'creation_algorithm_version': '1.1',  # Updated version for duration constraint
                'max_individual_video_duration_seconds': self.MAX_VIDEO_DURATION_SECONDS,
                'selection_criteria': {
                    'max_annual_usage': self.MAX_ANNUAL_USAGE,
                    'max_video_duration_seconds': self.MAX_VIDEO_DURATION_SECONDS,
                    'retention_weight': self.RETENTION_WEIGHT,
                    'view_count_weight': self.VIEW_COUNT_WEIGHT,
                    'freshness_weight': self.FRESHNESS_WEIGHT
                }
            },
            'export_data': {
                'last_exported': None,
                'export_count': 0
            }
        }

        return compilation_doc

    def _generate_compilation_title(self, videos: List[Dict], prefix: str, duration: int, compilation_type: str = "default") -> str:
        """Generate compilation title based on compilation type"""
        if not videos:
            return f"{prefix} - Compilation ({duration} min) - {datetime.now().strftime('%Y%m%d')}"

        # Remove existing suffixes to avoid duplication
        suffixes_to_remove = [
            " | Mega Compilation | D Billions Kids Songs",
            " | D Billions Kids Songs"
        ]

        if compilation_type == 'live':
            # For Live compilations: "Live" + names of two first videos without ending " | D Billions Kids Songs"
            first_video_title = videos[0].get('title', 'Untitled Video')
            second_video_title = videos[1].get('title', 'Untitled Video') if len(videos) > 1 else 'Video'

            # Clean titles
            for suffix in suffixes_to_remove:
                if first_video_title.endswith(suffix):
                    first_video_title = first_video_title[:-len(suffix)]
                if second_video_title.endswith(suffix):
                    second_video_title = second_video_title[:-len(suffix)]

            return f"Live | {first_video_title} | {second_video_title}"
        else:
            # Default compilation logic: first video title + suffix
            first_video_title = videos[0].get('title', 'Untitled Video')

            for suffix in suffixes_to_remove:
                if first_video_title.endswith(suffix):
                    first_video_title = first_video_title[:-len(suffix)]

            return f"{first_video_title} | Mega Compilation | D Billions Kids Songs"

    def _analyze_category_usage(self, selected_videos: List[Dict],
                                categorized_videos: Dict[VideoCategory, List[Dict]]) -> Dict:
        """Analyze which video categories were used in the final selection"""
        category_usage = {category.value: 0 for category in VideoCategory}

        # Create reverse lookup for video categories
        video_to_category = {}
        for category, videos in categorized_videos.items():
            for video in videos:
                video_to_category[video['video_id']] = category

        # Count usage by category
        for video in selected_videos:
            category = video_to_category.get(video['video_id'])
            if category:
                category_usage[category.value] += 1

        return category_usage

    def get_compilation_stats(self) -> Dict:
        """Get statistics about video eligibility and filtering"""
        all_videos = list(self.videos_collection.find({}))

        stats = {
            'total_videos': len(all_videos),
            'compilation_videos_flagged': 0,
            'compilation_videos_by_title': 0,
            'compilation_videos_in_db': 0,
            'over_10_minutes': 0,
            'over_usage_limit': 0,
            'missing_metrics': 0,
            'eligible_videos': 0
        }

        for video in all_videos:
            # Check for is_compilation flag
            if video.get('is_compilation', False):
                stats['compilation_videos_flagged'] += 1
                continue

            # Check for compilation keywords in title
            video_title = video.get('title', '').lower()
            if any(keyword in video_title for keyword in ['compilation', 'best of', 'highlights', 'collection']):
                stats['compilation_videos_by_title'] += 1
                continue

            # Check if video exists in compilations collection
            if self.compilations_collection.find_one({'video_ids': video.get('video_id')}):
                stats['compilation_videos_in_db'] += 1
                continue

            if video.get('duration_seconds', 0) > self.MAX_VIDEO_DURATION_SECONDS:
                stats['over_10_minutes'] += 1
                continue

            usage_stats = video.get('compilation_usage_stats', {})
            if usage_stats.get('total_inclusions', 0) >= self.MAX_ANNUAL_USAGE:
                stats['over_usage_limit'] += 1
                continue

            if not all(key in video for key in ['retention_30s', 'view_count']):
                stats['missing_metrics'] += 1
                continue

            stats['eligible_videos'] += 1

        return stats

    def get_compilation_preview(self, compilation_id: str) -> Optional[Dict]:
        """Get detailed preview of a created compilation"""
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return None

            # Enrich with additional video details
            enriched_timestamps = []
            for timestamp_entry in compilation.get('timestamps', []):
                video_detail = self.videos_collection.find_one({
                    'video_id': timestamp_entry['video_id']
                })

                if video_detail:
                    enriched_entry = timestamp_entry.copy()
                    enriched_entry['video_details'] = {
                        'view_count': video_detail.get('view_count', 0),
                        'like_count': video_detail.get('like_count', 0),
                        'published_at': video_detail.get('published_at', ''),
                        'thumbnail_url': video_detail.get('thumbnail_url', ''),
                        'full_description': video_detail.get('description', '')[:200] + '...',
                        'duration_check': 'VALID' if video_detail.get('duration_seconds', 0) <= self.MAX_VIDEO_DURATION_SECONDS else 'OVER_LIMIT',
                        'is_compilation': video_detail.get('is_compilation', False),
                        'compilation_check': 'PASS' if not video_detail.get('is_compilation', False) else 'FAIL'
                    }
                    enriched_timestamps.append(enriched_entry)

            compilation['enriched_timestamps'] = enriched_timestamps
            return compilation

        except Exception as e:
            return None

    def update_compilation_status(self, compilation_id: str, new_status: CompilationStatus, done_by: Optional[str] = None) -> bool:
        """Update the status of a compilation with workflow validation"""
        try:
            # Get current compilation to validate status transition
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return False

            current_status = compilation.get('status')

            # Validate status transitions
            valid_transitions = {
                CompilationStatus.GENERATED.value: [CompilationStatus.TO_DO.value],
                CompilationStatus.TO_DO.value: [CompilationStatus.READY.value],
                CompilationStatus.READY.value: [CompilationStatus.UPLOADED.value],
                CompilationStatus.UPLOADED.value: []  # Final state
            }

            if new_status.value not in valid_transitions.get(current_status, []):
                return False

            # Prepare update fields
            update_fields = {
                'status': new_status.value,
                'updated_at': datetime.utcnow()
            }

            # If changing to "ready" status and done_by is provided, set the done_by field
            if new_status == CompilationStatus.READY and done_by:
                update_fields['done_by'] = done_by

            # Update the status
            result = self.user_compilations_collection.update_one(
                {'_id': ObjectId(compilation_id)},
                {'$set': update_fields}
            )
            return result.modified_count > 0
        except Exception as e:
            return False

    def delete_compilation(self, compilation_id: str) -> bool:
        """Delete a compilation and update video usage statistics"""
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return False

            # Only allow deletion of non-published compilations
            # if compilation.get('status') == CompilationStatus.PUBLISHED.value:
            #     return False

            # Extract video IDs from the compilation before deleting it
            video_ids_in_compilation = []
            for timestamp in compilation.get('timestamps', []):
                if timestamp.get('video_id'):
                    video_ids_in_compilation.append(timestamp['video_id'])

            # Delete the compilation
            result = self.user_compilations_collection.delete_one({
                '_id': ObjectId(compilation_id)
            })

            if result.deleted_count > 0 and video_ids_in_compilation:
                # Update usage statistics for all videos that were in this compilation
                try:
                    for video_id in video_ids_in_compilation:
                        self.usage_tracker.update_video_usage_stats(video_id)
                except Exception as stats_error:
                    # Log stats update error but don't fail the deletion
                    print(f"Warning: Failed to update usage stats after compilation deletion: {stats_error}")

            return result.deleted_count > 0

        except Exception as e:
            return False

    def get_user_compilations(self, user_id: str = "system", status_filter: Optional[str] = None) -> List[Dict]:
        """Get all compilations created by a specific user with optional status filtering"""
        query = {'created_by': user_id}

        if status_filter:
            query['status'] = status_filter

        compilations = list(self.user_compilations_collection.find(query)
                            .sort('created_at', -1))

        # Convert ObjectId to string for JSON serialization
        for comp in compilations:
            comp['_id'] = str(comp['_id'])

        return compilations


    def debug_video_counts(self):
        """Debug method to see video distribution"""
        all_videos = list(self.videos_collection.find({}))

        duration_ranges = {
            'under_1_min': 0,
            '1_to_5_min': 0,
            '5_to_10_min': 0,
            'over_10_min': 0,
            'no_duration': 0
        }

        for video in all_videos:
            duration = video.get('duration_seconds', 0)
            if duration == 0:
                duration_ranges['no_duration'] += 1
            elif duration < 60:
                duration_ranges['under_1_min'] += 1
            elif duration <= 300:
                duration_ranges['1_to_5_min'] += 1
            elif duration <= 600:
                duration_ranges['5_to_10_min'] += 1
            else:
                duration_ranges['over_10_min'] += 1

        print("VIDEO DURATION DISTRIBUTION:")
        for range_name, count in duration_ranges.items():
            print(f"  {range_name}: {count}")

        return duration_ranges

    def validate_compilation_constraints(self, selected_videos: List[Dict], duration_rounded: int) -> Dict[str, Any]:
        """
        Comprehensive validation of compilation constraints to ensure compliance
        with the 365-day rule and cross-duration usage rules.
        
        Args:
            selected_videos: List of videos in the compilation
            duration_rounded: Target duration in minutes
            
        Returns:
            Dict with validation results and detailed information
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'constraints_checked': {
                'first_video_365_day_rule': None,
                'cross_duration_usage': [],
                'first_video_usage_counts': {},
                'total_videos_used': len(selected_videos)
            },
            'recommendations': []
        }
        
        if not selected_videos:
            validation_result['valid'] = False
            validation_result['errors'].append("No videos provided for validation")
            return validation_result
        
        duration_key = f"{duration_rounded}min"
        first_video = selected_videos[0]
        first_video_id = first_video.get('video_id')
        one_year_ago = datetime.utcnow() - timedelta(days=365)
        
        # Validate first video 365-day constraint
        usage_stats = first_video.get('compilation_usage_stats', {})
        first_video_by_duration = usage_stats.get('first_video_by_duration', {})
        first_video_last_used_by_duration = usage_stats.get('first_video_last_used_by_duration', {})
        
        usage_count = first_video_by_duration.get(duration_key, 0)
        last_used_date = first_video_last_used_by_duration.get(duration_key)
        
        # Check if first video violates 365-day rule
        was_used_recently = False
        if last_used_date and isinstance(last_used_date, datetime):
            if last_used_date > one_year_ago:
                was_used_recently = True
                validation_result['valid'] = False
                validation_result['errors'].append(
                    f"First video '{first_video.get('title', 'Unknown')}' was used as first video in "
                    f"{duration_key} compilation on {last_used_date.strftime('%Y-%m-%d')}. "
                    f"Cannot be used again until {one_year_ago.strftime('%Y-%m-%d')}"
                )
        
        validation_result['constraints_checked']['first_video_365_day_rule'] = {
            'violated': was_used_recently,
            'usage_count': usage_count,
            'last_used_date': last_used_date.isoformat() if last_used_date else None,
            'eligible_after': (last_used_date + timedelta(days=365)).isoformat() if last_used_date else None
        }
        
        # Check cross-duration usage
        for video in selected_videos:
            video_id = video.get('video_id')
            video_title = video.get('title', 'Unknown')
            video_usage_stats = video.get('compilation_usage_stats', {})
            
            video_usage_by_duration = video_usage_stats.get('usage_by_duration', {})
            video_first_video_by_duration = video_usage_stats.get('first_video_by_duration', {})
            
            # Check usage counts across all durations
            duration_usage = []
            for dur_key, count in video_usage_by_duration.items():
                if count > 0:
                    duration_usage.append({
                        'duration': dur_key,
                        'total_usage_count': count,
                        'first_video_count': video_first_video_by_duration.get(dur_key, 0),
                        'is_current_duration': dur_key == duration_key
                    })
            
            if duration_usage:
                validation_result['constraints_checked']['cross_duration_usage'].append({
                    'video_id': video_id,
                    'video_title': video_title,
                    'position_in_compilation': selected_videos.index(video) + 1,
                    'usage_by_duration': duration_usage
                })
        
        # Check for potential issues
        for video_data in validation_result['constraints_checked']['cross_duration_usage']:
            video_id = video_data['video_id']
            current_duration_usage = None
            
            for dur_usage in video_data['usage_by_duration']:
                if dur_usage['is_current_duration']:
                    current_duration_usage = dur_usage
                    break
            
            # Check if video is overused in current duration
            if current_duration_usage and current_duration_usage['total_usage_count'] >= self.MAX_ANNUAL_USAGE:
                validation_result['warnings'].append(
                    f"Video '{video_data['video_title']}' has been used "
                    f"{current_duration_usage['total_usage_count']} times in {duration_key} compilations "
                    f"and is approaching the annual limit of {self.MAX_ANNUAL_USAGE}"
                )
        
        # Generate recommendations
        if not was_used_recently and usage_count == 0:
            validation_result['recommendations'].append(
                f"First video '{first_video.get('title', 'Unknown')}' has never been used as first video "
                f"in {duration_key} compilations - excellent choice for engagement"
            )
        
        # Check if we have enough variety in video sources
        total_unique_videos_used = len(set(video.get('video_id') for video in selected_videos))
        if total_unique_videos_used < len(selected_videos):
            validation_result['warnings'].append("Duplicate videos detected in compilation")
        
        return validation_result

    def debug_compilation_constraints(self, duration_minutes: int, max_results: int = 10) -> Dict[str, Any]:
        """
        Debug method to analyze video availability and constraint violations
        for a specific duration compilation.
        
        Args:
            duration_minutes: Target compilation duration
            max_results: Maximum number of results to return
            
        Returns:
            Detailed debug information about constraint violations and video availability
        """
        duration_rounded = round(duration_minutes / 5) * 5
        duration_key = f"{duration_rounded}min"
        one_year_ago = datetime.utcnow() - timedelta(days=365)
        current_date = datetime.utcnow()
        
        debug_info = {
            'duration_analyzed': duration_key,
            'analysis_date': current_date.isoformat(),
            'constraint_date_range': f"Videos used after {one_year_ago.strftime('%Y-%m-%d')} are restricted",
            'videos_analyzed': 0,
            'eligible_videos': 0,
            'restricted_videos': [],
            'never_used_first_video': [],
            'usage_stats': {
                'total_videos': 0,
                'videos_with_compilation_usage': 0,
                'videos_never_used': 0,
                'videos_restricted_by_365_day_rule': 0
            }
        }
        
        # Get all videos for analysis
        all_videos = list(self.videos_collection.find({}))
        debug_info['videos_analyzed'] = len(all_videos)
        
        for video in all_videos:
            video_id = video.get('video_id')
            video_title = video.get('title', 'Unknown')
            usage_stats = video.get('compilation_usage_stats', {})
            
            first_video_by_duration = usage_stats.get('first_video_by_duration', {})
            first_video_last_used_by_duration = usage_stats.get('first_video_last_used_by_duration', {})
            
            usage_count = first_video_by_duration.get(duration_key, 0)
            last_used_date = first_video_last_used_by_duration.get(duration_key)
            
            # Check if video has any compilation usage
            total_inclusions = usage_stats.get('total_inclusions', 0)
            if total_inclusions == 0:
                debug_info['usage_stats']['videos_never_used'] += 1
                if usage_count == 0:
                    debug_info['never_used_first_video'].append({
                        'video_id': video_id,
                        'title': video_title,
                        'retention_30s': video.get('retention_30s', 0),
                        'duration_seconds': video.get('duration_seconds', 0)
                    })
            else:
                debug_info['usage_stats']['videos_with_compilation_usage'] += 1
            
            # Check 365-day restriction
            was_used_recently = False
            if last_used_date and isinstance(last_used_date, datetime):
                if last_used_date > one_year_ago:
                    was_used_recently = True
                    eligible_date = last_used_date + timedelta(days=365)
                    
                    debug_info['videos_restricted_by_365_day_rule'] += 1
                    debug_info['restricted_videos'].append({
                        'video_id': video_id,
                        'title': video_title,
                        'last_used_as_first': last_used_date.isoformat(),
                        'restricted_until': eligible_date.isoformat(),
                        'usage_count_in_duration': usage_count,
                        'remaining_restriction_days': max(0, (eligible_date - current_date).days)
                    })
            
            # Count as eligible if not restricted
            if not was_used_recently:
                debug_info['eligible_videos'] += 1
        
        debug_info['usage_stats']['total_videos'] = len(all_videos)
        
        # Limit results to prevent overwhelming output
        debug_info['restricted_videos'] = debug_info['restricted_videos'][:max_results]
        debug_info['never_used_first_video'] = debug_info['never_used_first_video'][:max_results]
        
        return debug_info
