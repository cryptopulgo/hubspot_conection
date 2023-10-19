import subprocess
import os

def compress_pdf(input_file_path, power=3):
    """
    Function to compress PDF via Ghostscript command line utility.
    With power=3, it downsamples all images above 300 dpi to 300dpi
    and compresses the streams.
    """
    quality = {
        0: '/default',
        1: '/prepress',
        2: '/printer',
        3: '/ebook',
        4: '/screen'
    }

    # Generate output file path
    base_name, ext = os.path.splitext(input_file_path)
    output_file_path = f"{base_name}_compressed{ext}"

    # Basic Ghostscript command without optional parameters
    gs_command = "gswin64 -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/ebook -dNOPAUSE -dQUIET -dBATCH -sOutputFile={1} {2}"

    # Add parameters (if any) to Ghostscript command
    final_command = gs_command.format(quality[power], output_file_path, input_file_path)

    # Execute command.
    try:
        process = subprocess.run(final_command, check=True, shell=True)
        process.check_returncode()  # Check if command was successful.

    except subprocess.CalledProcessError as e:
        print("Sorry, there was an error:", e)
        os.remove(output_file_path)  # If error, delete created file
        exit(-1)

    print(f"Compressed file saved to: {output_file_path}")

compress_pdf('test.pdf', power=4)

