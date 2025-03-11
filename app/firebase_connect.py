import firebase_admin
from firebase_admin import credentials, db

def get_firebase_connection():
    """
    Initialize and return a connection to Firebase Realtime Database.
    """
    try:
        if not firebase_admin._apps:  # Prevent re-initializing Firebase
            cred = credentials.Certificate('./fb_key.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://chatdb-sampledb-default-rtdb.firebaseio.com/'
            })
        return db
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        return None
