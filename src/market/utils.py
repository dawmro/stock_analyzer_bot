from django.apps import apps

def batch_insert_stock_data(dataset, company_obj=None, batch_size=1000, verbose=False):
    """
    Batch inserts stock quote data into the database using bulk operations for efficiency.
    
    This function processes large datasets in chunks to minimize database roundtrips and 
    memory usage. It utilizes Django's bulk_create with conflict handling to ignore duplicate
    entries, making it suitable for frequent data synchronization tasks.
    
    Args:
        dataset (list): List of dictionaries containing stock quote data fields
        company_obj (Company): Django model instance representing the associated company
        batch_size (int): Intended batch size 
        verbose (bool): Enable progress output if True
        
    Returns:
        int: Total number of records processed
        
    Raises:
        Exception: If company_obj is not provided
    """
    # Validate required company object
    if not company_obj:
        raise Exception(f"Batch insertion failed: Invalid company object {company_obj}")
    
    # Get StockQuote model reference
    StockQuote = apps.get_model('market', 'StockQuote')
        
    # Process dataset in batches
    for i in range(0, len(dataset), batch_size):
        if verbose:
            print(f"Processing batch: {i} to {i + batch_size} for {company_obj}")
        
        # Extract current batch chunk
        batch_chunk = dataset[i:i + batch_size]
        chunked_quotes = []
        
        # Prepare model instances for current batch
        for data in batch_chunk:
            chunked_quotes.append(
                StockQuote(
                    company=company_obj, 
                    **data  # Unpack dictionary as keyword arguments
                )
            )
        
        # Bulk insert with conflict handling - skips duplicates
        StockQuote.objects.bulk_create(chunked_quotes, ignore_conflicts=True)
        
        if verbose:
            print(f"Completed batch: {i} to {i + batch_size} for {company_obj}")
    if verbose:
            print(f"Processed {len(dataset)} data points for {company_obj}")
    
    # Return total processed records count
    return len(dataset)


def batch_insert_stock_indicators(dataset, company_obj=None, batch_size=100, verbose=False):
    """
    Batch inserts stock indicator data into the database using bulk operations for efficiency.
    
    This function processes large datasets in chunks to minimize database roundtrips and 
    memory usage. It utilizes Django's bulk_create with conflict handling to ignore duplicate
    entries, making it suitable for frequent data synchronization tasks.
    
    Args:
        dataset (list): List of dictionaries containing stock indicator data fields
        company_obj (Company): Django model instance representing the associated company
        batch_size (int): Intended batch size (default: 100)
        verbose (bool): Enable progress output if True
        
    Returns:
        int: Total number of records processed
        
    Raises:
        Exception: If company_obj is not provided
    """
    # Validate required company object
    if not company_obj:
        raise Exception(f"Batch insertion failed: Invalid company object {company_obj}")
    
    # Get StockIndicator model reference
    StockIndicator = apps.get_model('market', 'StockIndicator')
    
    # Validate dataset structure
    if not dataset or not isinstance(dataset, list):
        if verbose:
            print("No indicator data to insert or invalid dataset format")
        return 0
    
    # Counters for progress tracking
    total_records = len(dataset)
    processed_records = 0
    
    # Process dataset in batches
    for i in range(0, total_records, batch_size):
        batch_start = i
        batch_end = min(i + batch_size, total_records)
        batch_chunk = dataset[batch_start:batch_end]
        
        if verbose:
            print(f"Processing indicator batch: {batch_start} to {batch_end} for {company_obj}")
        
        chunked_indicators = []
        skipped_records = 0
        
        # Prepare model instances for current batch with validation
        for data in batch_chunk:
            # Validate required fields
            if 'time' not in data or 'score' not in data or 'indicators' not in data:
                if verbose:
                    print(f"Skipping invalid record: {data}")
                skipped_records += 1
                continue
                
            try:
                chunked_indicators.append(
                    StockIndicator(
                        company=company_obj, 
                        time=data['time'],
                        score=data['score'],
                        indicators=data['indicators']
                    )
                )
            except Exception as e:
                if verbose:
                    print(f"Error preparing record: {str(e)}")
                skipped_records += 1
        
        # Bulk insert with conflict handling - skips duplicates
        try:
            StockIndicator.objects.bulk_create(chunked_indicators, ignore_conflicts=True)
            processed_records += len(chunked_indicators)
            if verbose:
                success_count = len(chunked_indicators)
                print(f"Inserted {success_count} indicators, skipped {skipped_records} records in batch {batch_start}-{batch_end}")
        except Exception as e:
            if verbose:
                print(f"Error inserting batch: {str(e)}")
    
    if verbose:
        print(f"Processed {processed_records} out of {total_records} indicator records for {company_obj}")
        if processed_records < total_records:
            print(f"Skipped {total_records - processed_records} records due to errors or missing fields")
    
    # Return total processed records count
    return processed_records
