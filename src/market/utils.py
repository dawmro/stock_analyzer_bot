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
