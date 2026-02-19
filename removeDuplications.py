def remove_duplications(spark, input_file):
    df = spark.read.format("csv").load(input_file, header=True, inferSchema=True)
    df = df.dropDuplicates()
    df = df.drop("Navigational status", "Heading", "ROT", "IMO", "Callsign", 
            "Name", "Cargo type", "Width", "Length", "Draught", "Destination", "ETA", 
            "Data source type", "A", "B", "C", "D", "Type of position fixing device")
    df = df.filter(df["Type of mobile"] == "Class A")
    return df