# Enc-fragment-extractor

This project is a GUI application for extracting fragments and using state-of-the-art features to determine encrypted fragments from unencrypted ones.

## Features
- Extracting fragments from a whole dataset
- Extracting fragments from each file
- Extracting features in parallel
- State-of-the-art models features such as NIST 800-22, HEDGE, EnCoD, and BLFCP

## Graphical User Interface
<img src="screenshots/fragment_extractor.png" alt="fragment_extractor" width="300" /> <img src="screenshots/fragment_extractor_2.png" alt="fragment_extractor_2" width="300" />
<img src="screenshots/feature_extractor.png" alt="feature_extractor" width="300" />

## Downloads
1.  You can directly download the portable .exe for 64-bit Windows OS from the [Releases page](link-to-your-release-page).

2.  Alternatively, you can follow the instructions below to run it on any OS:

    First, download the repository and make sure you have Python installed.
    Go to the project directory, open a command prompt, and install the required packages:
    ```bash
    pip install -r requirements.txt
    ```
    Run the main application with:
    ```bash
    python MainGui.py
    ```

## Documentation
- Open the app and read the `Options -> Guidance` and `Options -> About` sections.

## License
- GPL 3.0

