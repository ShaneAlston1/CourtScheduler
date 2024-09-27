#!/bin/bash

# Define variables
SERVICE_NAME="calenderProcessor"
SCRIPT_PATH="/usr/local/bin/calenderProcessor.py"
SERVICE_PATH="/etc/systemd/system/$SERVICE_NAME.service"
PYTHON_PATH="/usr/bin/python3"
USERNAME=$(whoami)

# Step 1: Move the Python script to /usr/local/bin/
echo "Moving the Python script to /usr/local/bin/"
sudo mv ./calenderProcessor.py $SCRIPT_PATH
sudo chmod +x $SCRIPT_PATH

# Step 2: Create the systemd service file
echo "Creating the systemd service file at $SERVICE_PATH"
sudo bash -c "cat > $SERVICE_PATH" <<EOL
[Unit]
Description=Calendar Processor Service
After=network.target

[Service]
ExecStart=$PYTHON_PATH $SCRIPT_PATH
WorkingDirectory=/usr/local/bin
Restart=always
User=$USERNAME
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOL

# Step 3: Enable and start the systemd service
echo "Reloading systemd and enabling the $SERVICE_NAME service"
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME.service
sudo systemctl start $SERVICE_NAME.service

# Step 4: Set up a cron job to run the script every hour
echo "Setting up a cron job to run the script every hour"
(crontab -l 2>/dev/null; echo "0 * * * * $PYTHON_PATH $SCRIPT_PATH") | crontab -

# Final status check
echo "Checking the status of the service"
sudo systemctl status $SERVICE_NAME.service

echo "Setup complete! The service is now running and a cron job is set to execute it every hour."
