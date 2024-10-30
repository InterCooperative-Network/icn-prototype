import os
from flask import Flask, request, jsonify, send_file
from flask_jwt_extended import (
    JWTManager,
    create_access_token,
    jwt_required,
    get_jwt_identity,
)
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from dotenv import load_dotenv
from io import BytesIO

# Import core modules
from blockchain.blockchain import Blockchain
from did.did import DID, DIDRegistry
from system.governance import Governance, Proposal
from system.marketplace import Marketplace
from system.storage import DistributedStorage

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY")  # Load JWT secret from .env
jwt = JWTManager(app)

# Initialize core components
blockchain = Blockchain()
did_registry = DIDRegistry()
governance = Governance(blockchain)
marketplace = Marketplace(blockchain, did_registry)
storage = DistributedStorage(blockchain)

# In-memory user database (development only)
users_db = {}


# Role-based access decorator
def role_required(required_role):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            current_user = get_jwt_identity()
            if current_user["role"] != required_role:
                return jsonify({"message": "Access forbidden: insufficient role"}), 403
            return f(*args, **kwargs)

        return wrapper

    return decorator


# User registration
@app.route("/register", methods=["POST"])
def register():
    data = request.json
    username = data.get("username")
    password = data.get("password")
    role = data.get("role", "user")

    if not username or not password:
        return jsonify({"message": "Username and password are required"}), 400
    if username in users_db:
        return jsonify({"message": "User already exists"}), 400

    # Generate DID for the new user
    did = DID()
    did_id = did_registry.register_did(did)

    # Hash the password and store user info
    hashed_password = generate_password_hash(password)
    users_db[username] = {"password": hashed_password, "role": role, "did": did_id}

    return jsonify(
        {"message": f"User {username} registered successfully", "did": did_id}
    ), 201


# User login
@app.route("/login", methods=["POST"])
def login():
    data = request.json
    username = data.get("username")
    password = data.get("password")

    user = users_db.get(username)
    if not user or not check_password_hash(user["password"], password):
        return jsonify({"message": "Invalid username or password"}), 401

    access_token = create_access_token(
        identity={"username": username, "role": user["role"], "did": user["did"]}
    )
    return jsonify(access_token=access_token), 200


# Proposal creation
@app.route("/create_proposal", methods=["POST"])
@jwt_required()
def create_proposal():
    current_user = get_jwt_identity()
    data = request.json

    if not data.get("title") or not data.get("description"):
        return jsonify({"message": "Title and description are required"}), 400

    proposal = Proposal(
        id=f"proposal_{len(governance.proposals) + 1}",
        title=data["title"],
        description=data["description"],
        creator=current_user["did"],
        proposal_type=data.get("type", "simple"),
        options=data.get("options", ["Yes", "No"]),
        amount=data.get("amount"),
        stages=data.get("stages"),
    )
    proposal_id = governance.create_proposal(proposal)
    return jsonify({"message": "Proposal created", "proposal_id": proposal_id}), 201


# Vote on a proposal
@app.route("/vote", methods=["POST"])
@jwt_required()
def vote():
    current_user = get_jwt_identity()
    proposal_id = request.json.get("proposal_id")
    vote = request.json.get("vote")
    voting_power = request.json.get("voting_power", 1)

    if not proposal_id or not vote:
        return jsonify({"message": "Proposal ID and vote are required"}), 400

    if governance.cast_vote(proposal_id, vote, current_user["did"], voting_power):
        return jsonify({"message": "Vote cast successfully"}), 200
    return jsonify({"message": "Failed to cast vote"}), 400


# Finalize a proposal (admin-only)
@app.route("/finalize_proposal", methods=["POST"])
@jwt_required()
@role_required("admin")
def finalize_proposal():
    proposal_id = request.json.get("proposal_id")

    if not proposal_id:
        return jsonify({"message": "Proposal ID is required"}), 400

    if governance.finalize_proposal(proposal_id):
        return jsonify({"message": "Proposal finalized successfully"}), 200
    return jsonify({"message": "Failed to finalize proposal"}), 400


# Create a marketplace listing
@app.route("/create_listing", methods=["POST"])
@jwt_required()
def create_listing():
    current_user = get_jwt_identity()
    data = request.json

    if not data.get("item") or not data.get("price"):
        return jsonify({"message": "Item and price are required"}), 400

    listing_id = marketplace.create_listing(
        f"listing_{len(marketplace.listings) + 1}",
        current_user["did"],
        data["item"],
        data["price"],
    )
    if listing_id:
        return jsonify({"message": "Listing created", "listing_id": listing_id}), 201
    return jsonify({"message": "Failed to create listing"}), 400


# Place an order
@app.route("/place_order", methods=["POST"])
@jwt_required()
def place_order():
    current_user = get_jwt_identity()
    listing_id = request.json.get("listing_id")

    if not listing_id:
        return jsonify({"message": "Listing ID is required"}), 400

    order_id = f"order_{len(marketplace.orders) + 1}"
    if marketplace.place_order(order_id, current_user["did"], listing_id):
        return jsonify({"message": "Order placed", "order_id": order_id}), 201
    return jsonify({"message": "Failed to place order"}), 400


# Complete an order
@app.route("/complete_order", methods=["POST"])
@jwt_required()
def complete_order():
    order_id = request.json.get("order_id")

    if not order_id:
        return jsonify({"message": "Order ID is required"}), 400

    completed_order = marketplace.complete_order(order_id)
    if completed_order:
        return jsonify(
            {"message": "Order completed", "order": completed_order.__dict__}
        ), 200
    return jsonify({"message": "Failed to complete order"}), 400


# File storage
@app.route("/store_file", methods=["POST"])
@jwt_required()
def store_file():
    file = request.files.get("file")

    if not file:
        return jsonify({"message": "File is required"}), 400

    file_hash = storage.store_file(file.filename, file.read())
    return jsonify({"message": "File stored", "file_hash": file_hash}), 201


@app.route("/retrieve_file/<file_hash>", methods=["GET"])
@jwt_required()
def retrieve_file(file_hash):
    file_data = storage.retrieve_file(file_hash)
    if file_data:
        return send_file(
            BytesIO(file_data), download_name=file_hash, as_attachment=True
        ), 200
    return jsonify({"message": "File not found"}), 404


if __name__ == "__main__":
    app.run(debug=True)
