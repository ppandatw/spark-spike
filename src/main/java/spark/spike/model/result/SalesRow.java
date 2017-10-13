package spark.spike.model.result;

import java.io.Serializable;

public class SalesRow implements Serializable {
	private String customer_number;
	private String store_number;
	private String department_number;
	private Double total_amount;
	
	public String getCustomer_number() {
		return customer_number;
	}
	
	public SalesRow setCustomer_number(String customer_number) {
		this.customer_number = customer_number;
		return this;
	}
	
	public String getStore_number() {
		return store_number;
	}
	
	public SalesRow setStore_number(String store_number) {
		this.store_number = store_number;
		return this;
	}
	
	public String getDepartment_number() {
		return department_number;
	}
	
	public SalesRow setDepartment_number(String department_number) {
		this.department_number = department_number;
		return this;
	}
	
	public Double getTotal_amount() {
		return total_amount;
	}
	
	public SalesRow setTotal_amount(Double total_amount) {
		this.total_amount = total_amount;
		return this;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		
		SalesRow salesRow = (SalesRow) o;
		
		if (!customer_number.equals(salesRow.customer_number)) return false;
		if (!store_number.equals(salesRow.store_number)) return false;
		if (!department_number.equals(salesRow.department_number)) return false;
		return total_amount.equals(salesRow.total_amount);
	}
	
	@Override
	public int hashCode() {
		int result = customer_number.hashCode();
		result = 31 * result + store_number.hashCode();
		result = 31 * result + department_number.hashCode();
		result = 31 * result + total_amount.hashCode();
		return result;
	}
	
	public static String getColumnNames() {
		return " ( customer_number, store_number, department_number, total_amount ) ";
	}
	
	@Override
	public String toString() {
		return " ( '" + customer_number
					   + "' , '" + store_number
					   + "' , '" + department_number
					   + "' , " + total_amount +
					   " )";
		
	}
}
