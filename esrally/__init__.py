import os

# Allow an alternative program name be set in case Rally is invoked a wrapper script
PROGRAM_NAME = os.getenv("RALLY_ALTERNATIVE_BINARY_NAME", "esrally")
