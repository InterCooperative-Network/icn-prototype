from typing import Dict

class Listing:
    def __init__(self, id, seller, item, price):
        self.id = id
        self.seller = seller
        self.item = item
        self.price = price

class Order:
    def __init__(self, id, buyer, listing):
        self.id = id
        self.buyer = buyer
        self.listing = listing
        self.status = "pending"

class Marketplace:
    def __init__(self, blockchain, did_registry):
        self.blockchain = blockchain
        self.did_registry = did_registry
        self.listings: Dict[str, Listing] = {}
        self.orders: Dict[str, Order] = {}

    def create_listing(self, listing_id, seller_did, item, price):
        seller = self.did_registry.resolve_did(seller_did)
        if not seller:
            return None
        
        listing = Listing(listing_id, seller_did, item, price)
        self.listings[listing_id] = listing
        self.blockchain.add_new_block(f"Listing created: {listing_id}", 1)  # Assume shard 1 for marketplace
        return listing_id

    def remove_listing(self, listing_id, seller_did):
        listing = self.listings.get(listing_id)
        if listing and listing.seller == seller_did:
            del self.listings[listing_id]
            self.blockchain.add_new_block(f"Listing removed: {listing_id}", 1)
            return True
        return False

    def place_order(self, order_id, buyer_did, listing_id):
        listing = self.listings.get(listing_id)
        buyer = self.did_registry.resolve_did(buyer_did)
        if not listing or not buyer:
            return False
        
        order = Order(order_id, buyer_did, listing)
        self.orders[order_id] = order
        self.blockchain.add_new_block(f"Order placed: {order_id}", 1)
        return True

    def complete_order(self, order_id):
        order = self.orders.get(order_id)
        if not order:
            return None
        
        # In a real system, you'd implement escrow release here
        order.status = "completed"
        self.blockchain.add_new_block(f"Order completed: {order_id}", 1)
        return order

    def get_listing(self, listing_id):
        return self.listings.get(listing_id)

    def get_order(self, order_id):
        return self.orders.get(order_id)

    def list_listings(self):
        return self.listings

    def list_orders(self):
        return self.orders

    def get_seller_reputation(self, seller_did):
        seller = self.did_registry.resolve_did(seller_did)
        if seller:
            return seller.get_reputation_scores().get("marketplace", 0)
        return 0