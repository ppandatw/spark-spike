package spark.spike;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class CustomerBranch implements Serializable{
    private CustomerIdentifier customerIdentifier;
    private String branch;
}
