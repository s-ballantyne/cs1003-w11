import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AscendingComparator extends WritableComparator {
	public int compare(WritableComparable a, WritableComparable b) {
		return 0;
	}
}
