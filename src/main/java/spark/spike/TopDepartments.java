package spark.spike;

public class TopDepartments {
    private String customer_no;
    private String store_no;
    private String departments;

    public TopDepartments(String customerNumber, String storeNumber, String topDepartments) {
        this.customer_no = customerNumber;
        this.store_no = storeNumber;
        this.departments = topDepartments;
    }

    public String getCustomer_no() {
        return customer_no;
    }

    public String getStore_no() {
        return store_no;
    }

    public String getDepartments() {
        return departments;
    }
}
