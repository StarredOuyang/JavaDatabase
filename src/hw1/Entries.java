package hw1;

public class Entries{
	//might need are own hash function?? using new entry to expect same w/r mapping if all 3
	//instance vars are the same
	private int page_id, table_id, transaction_id;
	public Entries(int page_id, int table_id, int transaction_id) {
		this.page_id = page_id;
		this.table_id = table_id;
		this.transaction_id = transaction_id;

	}
	public int get_page_id(){
		return page_id;
	}
	public int get_table_id(){
		return table_id;
	}
	public int get_transaction_id(){
		return transaction_id;
	}
	 @Override
	    public int hashCode() {
	        int result = 17;
	        result = 31 * page_id;
	        result *= 31 * table_id%7;
	        result *= 31 * table_id;
	        return result;
	    }
	
}