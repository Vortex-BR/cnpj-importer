from app.config import Settings
from app.factory import create_app
from app.logging_config import configure_logging


settings = Settings()
configure_logging(settings.log_level)
app = create_app(settings=settings)

