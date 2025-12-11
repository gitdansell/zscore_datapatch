import duckdb
import os
import glob

def main():   
    
    source_dirs = list(map(os.path.expandvars, 
    [
        r"%USERPROFILE%\Downloads\d_us_txt\data\daily\us\nasdaq stocks\1",
        r"%USERPROFILE%\Downloads\d_us_txt\data\daily\us\nasdaq stocks\2",
        r"%USERPROFILE%\Downloads\d_us_txt\data\daily\us\nasdaq stocks\3",
        r"%USERPROFILE%\Downloads\d_us_txt\data\daily\us\nyse stocks\1",
        r"%USERPROFILE%\Downloads\d_us_txt\data\daily\us\nyse stocks\2",
        r"%USERPROFILE%\Downloads\d_us_txt\data\daily\us\nysemkt stocks"
    ]))

    """
        One-time code to clean up the headers in all the files (remove the <>, ex: <TICKER> becomes TICKER)

        for folder in source_dirs:
            for filepath in glob.glob(os.path.join(folder, "*.us.txt")):
                with open(filepath, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                if not lines:
                    continue  # skip empty files

                # Clean header only (first line)
                header = lines[0].replace("<", "").replace(">", "")

                # Rewrite file with cleaned header + rest unchanged
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(header)
                    f.writelines(lines[1:])
    """

    all_globs = [os.path.join(dir, "*.us.txt") for dir in source_dirs]

    with duckdb.connect("us_stock_hist.duckdb") as conn:

        # Step 1 - read raw
        # all_globs must be passed in as a 1-element sequence. That element can be a list. So pass in as [all_globs] 
        # or (all_globs). If passed as all_globs, each glob in the list is treated as a separate parameter, causing an error. 
        conn.execute(f"""
            CREATE OR REPLACE TABLE us_hist_raw AS
            SELECT * FROM read_csv_auto(
                ?,                
                delim=',',
                header=true,
                union_by_name=true,
                SAMPLE_SIZE=-1
            )
        """, [all_globs])

        # Step 2 - normalize
        conn.execute("""
            CREATE OR REPLACE TABLE us_hist AS
            SELECT 
                TICKER::TEXT AS TICKER,                
                strptime(DATE::VARCHAR, '%Y%m%d') AS DATE,                  
                OPEN::DOUBLE AS OPEN,
                HIGH::DOUBLE AS HIGH,
                LOW::DOUBLE AS LOW,
                CLOSE::DOUBLE AS CLOSE,
                VOL::DOUBLE AS VOL,                
            FROM us_hist_raw
        """)                                                  

        print(conn.execute("select count(*) from us_hist").fetchall())


if __name__ == "__main__":
    main()
