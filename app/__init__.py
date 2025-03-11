from flask import Flask
from flask_cors import CORS
from app.routes import routes  # Import the routes Blueprint

def create_app():
    """
    Create and configure the Flask application.
    """
    app = Flask(__name__)

    # Enable Cross-Origin Resource Sharing (CORS)
    CORS(app)

    # Register routes
    app.register_blueprint(routes)

    return app
