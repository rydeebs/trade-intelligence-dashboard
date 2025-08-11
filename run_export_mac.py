from data_exporter import PowerBIDataExporter
import shutil
from pathlib import Path
import os
from datetime import datetime

# Mac OneDrive path (if you have OneDrive installed)
# Change this to your actual OneDrive path on Mac
ONEDRIVE_PATH = os.path.expanduser("~/OneDrive/PowerBI_eCommerce_Data")

# Alternative: Just save to local folder for now
LOCAL_OUTPUT_PATH = "./powerbi_csv_files"

def main():
    print("🌍 eCommerce Expansion Data Export for Power BI (Mac Version)")
    print("=" * 60)
    
    # Ask user where to save files
    print("Where do you want to save the CSV files?")
    print("1. Local folder (./powerbi_csv_files)")
    print("2. OneDrive folder (if OneDrive is installed)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "2":
        # Check if OneDrive exists on Mac
        possible_onedrive_paths = [
            os.path.expanduser("~/OneDrive"),
            os.path.expanduser("~/OneDrive - Personal"),
            os.path.expanduser("~/Library/CloudStorage/OneDrive-Personal"),
            os.path.expanduser("~/Library/CloudStorage/OneDrive-ShipBobInc")
        ]
        
        onedrive_found = False
        for path in possible_onedrive_paths:
            if os.path.exists(path):
                output_path = f"{path}/PowerBI_eCommerce_Data"
                onedrive_found = True
                print(f"📂 Found OneDrive at: {path}")
                break
        
        if not onedrive_found:
            print("❌ OneDrive not found on Mac. Using local folder instead.")
            output_path = LOCAL_OUTPUT_PATH
    else:
        output_path = LOCAL_OUTPUT_PATH
    
    # Step 1: Export data from World Bank API
    print("\n📊 Step 1: Exporting data from World Bank API...")
    print("(This will take 30-60 minutes for 100+ countries)")
    
    exporter = PowerBIDataExporter(output_dir="temp_csv_export")
    success = exporter.export_all_data()
    
    if not success:
        print("❌ Data export failed. Check error messages above.")
        return
    
    # Step 2: Copy to final destination
    print(f"\n📁 Step 2: Copying CSV files to: {output_path}")
    
    # Create output folder
    output_folder = Path(output_path)
    output_folder.mkdir(parents=True, exist_ok=True)
    
    # Copy all CSV files
    temp_folder = Path("temp_csv_export")
    csv_files = list(temp_folder.glob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found to copy")
        return
    
    copied_files = []
    for csv_file in csv_files:
        try:
            destination = output_folder / csv_file.name
            shutil.copy2(csv_file, destination)
            copied_files.append(csv_file.name)
            print(f"✅ Copied {csv_file.name}")
        except Exception as e:
            print(f"❌ Failed to copy {csv_file.name}: {e}")
    
    # Create timestamp file
    timestamp_file = output_folder / "last_updated.txt"
    with open(timestamp_file, 'w') as f:
        f.write(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Files updated: {len(copied_files)}\n")
        f.write(f"Export location: {output_path}\n")
        f.write("Files:\n")
        for file in copied_files:
            f.write(f"  - {file}\n")
    
    # Clean up temp folder
    try:
        shutil.rmtree(temp_folder)
        print("🧹 Cleaned up temporary files")
    except:
        pass
    
    print(f"\n🎉 SUCCESS! {len(copied_files)} files saved")
    print(f"📂 Location: {output_path}")
    
    # Show next steps
    if "OneDrive" in output_path:
        print("\n📋 Next steps:")
        print("1. OneDrive will sync these files to cloud")
        print("2. On your work laptop, files will appear in OneDrive")
        print("3. Connect Power BI to the OneDrive folder")
    else:
        print("\n📋 Next steps:")
        print("1. Copy the files from this folder to your work laptop")
        print("2. Upload to OneDrive on your work laptop")
        print("3. Connect Power BI to the OneDrive folder")
    
    print("\n📄 Files created:")
    for file in copied_files:
        print(f"  ✓ {file}")

if __name__ == "__main__":
    # Check if required packages are installed
    try:
        import pandas
        import numpy
        import requests
        print("✅ Required packages installed")
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        print("Install with: pip install pandas numpy requests")
        exit(1)
    
    main()