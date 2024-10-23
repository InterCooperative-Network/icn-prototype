# system/reputation.py

from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, field
import math

logger = logging.getLogger(__name__)

@dataclass
class ReputationEvent:
    """Represents a reputation-changing event."""
    category: str
    score: float
    timestamp: datetime
    evidence: Optional[Dict] = None
    decay_rate: float = 0.1

    def get_current_value(self, current_time: datetime) -> float:
        """Calculate the current value of the reputation event with decay."""
        age = (current_time - self.timestamp).total_seconds() / (24 * 3600)  # age in days
        return self.score * math.exp(-self.decay_rate * age)

class ReputationCategory:
    """Manages reputation for a specific category."""
    
    def __init__(self, name: str, weight: float = 1.0, 
                 decay_rate: float = 0.1):
        self.name = name
        self.weight = weight
        self.decay_rate = decay_rate
        self.events: List[ReputationEvent] = []
        self.minimum_score = 0.0
        self.maximum_score = float('inf')

    def add_event(self, score: float, evidence: Optional[Dict] = None) -> None:
        """Add a new reputation event."""
        event = ReputationEvent(
            category=self.name,
            score=score,
            timestamp=datetime.now(),
            evidence=evidence,
            decay_rate=self.decay_rate
        )
        self.events.append(event)

    def get_current_score(self) -> float:
        """Calculate current score with decay."""
        current_time = datetime.now()
        total_score = sum(event.get_current_value(current_time) 
                         for event in self.events)
        return max(min(total_score, self.maximum_score), self.minimum_score)

    def prune_old_events(self, max_age_days: int = 30) -> None:
        """Remove events older than specified age."""
        cutoff_time = datetime.now() - timedelta(days=max_age_days)
        self.events = [event for event in self.events 
                      if event.timestamp >= cutoff_time]

class ReputationSystem:
    """Enhanced reputation system for ICN."""
    
    def __init__(self):
        self.categories: Dict[str, ReputationCategory] = {
            "validation": ReputationCategory("validation", 1.0, 0.1),
            "voting": ReputationCategory("voting", 1.2, 0.05),
            "proposal_creation": ReputationCategory("proposal_creation", 1.5, 0.03),
            "development": ReputationCategory("development", 2.0, 0.02),
            "budget_management": ReputationCategory("budget_management", 1.8, 0.08),
            "community_building": ReputationCategory("community_building", 1.3, 0.04),
            "resource_sharing": ReputationCategory("resource_sharing", 1.4, 0.06),
            "cooperative_growth": ReputationCategory("cooperative_growth", 1.6, 0.03),
            "conflict_resolution": ReputationCategory("conflict_resolution", 1.7, 0.05),
            "sustainability": ReputationCategory("sustainability", 1.5, 0.04),
            "participation": ReputationCategory("participation", 1.1, 0.07),
            "innovation": ReputationCategory("innovation", 1.9, 0.02),
        }
        self.user_reputations: Dict[str, Dict[str, ReputationCategory]] = {}
        self.user_metadata: Dict[str, Dict] = {}
        self.registered_users: Set[str] = set()
        
    def add_user(self, user_id: str, metadata: Optional[Dict] = None) -> None:
        """Initialize reputation categories for a new user."""
        if user_id not in self.user_reputations:
            self.user_reputations[user_id] = {
                name: ReputationCategory(name, cat.weight, cat.decay_rate)
                for name, cat in self.categories.items()
            }
            self.user_metadata[user_id] = metadata or {}
            self.registered_users.add(user_id)
            logger.info(f"Initialized reputation for user: {user_id}")

    def remove_user(self, user_id: str) -> bool:
        """Remove a user from the reputation system."""
        if user_id in self.user_reputations:
            del self.user_reputations[user_id]
            del self.user_metadata[user_id]
            self.registered_users.remove(user_id)
            logger.info(f"Removed user: {user_id}")
            return True
        return False

    def get_all_users(self) -> List[str]:
        """Get list of all registered users."""
        return list(self.registered_users)

    def update_reputation(self, user_id: str, score: float, 
                         category: str, evidence: Optional[Dict] = None) -> bool:
        """Update a user's reputation with evidence."""
        if user_id not in self.user_reputations:
            logger.warning(f"Unknown user: {user_id}")
            return False
        
        if category not in self.user_reputations[user_id]:
            logger.warning(f"Unknown category: {category}")
            return False

        if score == 0:
            return True

        self.user_reputations[user_id][category].add_event(score, evidence)
        logger.info(f"Updated reputation for {user_id} in {category}: {score}")
        
        # Update participation category automatically
        if category != "participation":
            self.user_reputations[user_id]["participation"].add_event(
                abs(score) * 0.1,  # Small participation score for any activity
                {"source_category": category}
            )
        
        return True

    def get_reputation(self, user_id: str) -> Dict[str, float]:
        """Get current reputation scores for a user."""
        if user_id not in self.user_reputations:
            return {}
        
        return {
            category: rep_category.get_current_score()
            for category, rep_category in self.user_reputations[user_id].items()
        }

    def get_total_reputation(self, user_id: str) -> float:
        """Calculate total weighted reputation score."""
        if user_id not in self.user_reputations:
            return 0.0
        
        scores = self.get_reputation(user_id)
        weighted_scores = [
            score * self.categories[category].weight
            for category, score in scores.items()
        ]
        return sum(weighted_scores)

    def get_user_ranking(self, category: Optional[str] = None) -> List[tuple]:
        """Get users ranked by reputation in a category or overall."""
        if category and category not in self.categories:
            return []

        rankings = []
        for user_id in self.registered_users:
            if category:
                score = self.get_reputation(user_id).get(category, 0)
            else:
                score = self.get_total_reputation(user_id)
            rankings.append((user_id, score))

        return sorted(rankings, key=lambda x: x[1], reverse=True)

    def apply_decay(self) -> None:
        """Apply reputation decay to all users."""
        current_time = datetime.now()
        for user_reputations in self.user_reputations.values():
            for category in user_reputations.values():
                category.prune_old_events()

    def get_reputation_history(self, user_id: str, category: str,
                             days: int = 30) -> List[Dict]:
        """Get historical reputation events for a category."""
        if user_id not in self.user_reputations:
            return []
        
        category_rep = self.user_reputations[user_id].get(category)
        if not category_rep:
            return []

        cutoff_time = datetime.now() - timedelta(days=days)
        history = []
        
        for event in category_rep.events:
            if event.timestamp >= cutoff_time:
                history.append({
                    'timestamp': event.timestamp,
                    'score': event.score,
                    'evidence': event.evidence,
                    'current_value': event.get_current_value(datetime.now())
                })
                
        return sorted(history, key=lambda x: x['timestamp'])

    def get_category_thresholds(self, category: str) -> Optional[Dict[str, float]]:
        """Get the minimum and maximum thresholds for a category."""
        if category not in self.categories:
            return None
            
        return {
            'minimum': self.categories[category].minimum_score,
            'maximum': self.categories[category].maximum_score
        }

    def set_category_thresholds(self, category: str, 
                              minimum: float, maximum: float) -> bool:
        """Set the minimum and maximum thresholds for a category."""
        if category not in self.categories:
            return False
            
        if minimum > maximum:
            return False
            
        self.categories[category].minimum_score = minimum
        self.categories[category].maximum_score = maximum
        return True

    def get_system_stats(self) -> Dict:
        """Get system-wide reputation statistics."""
        stats = {
            'total_users': len(self.registered_users),
            'total_events': 0,
            'category_averages': {},
            'top_users': self.get_user_ranking()[:10]
        }
        
        for category in self.categories:
            scores = []
            total_events = 0
            for user_id in self.registered_users:
                user_rep = self.user_reputations.get(user_id, {}).get(category)
                if user_rep:
                    scores.append(user_rep.get_current_score())
                    total_events += len(user_rep.events)
            
            if scores:
                stats['category_averages'][category] = sum(scores) / len(scores)
            else:
                stats['category_averages'][category] = 0
            
            stats['total_events'] += total_events
        
        return stats