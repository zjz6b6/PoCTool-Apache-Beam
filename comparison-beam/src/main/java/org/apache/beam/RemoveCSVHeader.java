//package org.apache.beam;
//
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
//import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
//
////public class RemoveCSVHeader {
//	 // The Filter class
////	  static class FilterCSVHeaderFn extends DoFn<String, String> {
//	    /**
//		 * 
//		 */
////		private static final long serialVersionUID = 1L;
////		String headerFilter;
//
////	    public FilterCSVHeaderFn(String headerFilter) {
////	      this.headerFilter = headerFilter;
////	    }
////
////	    @ProcessElement
////	    public void processElement(ProcessContext c) {
////	      String row = (String) c.element();
////	      // Filter out elements that match the header
////	      if (!row.equals(this.headerFilter)) {
////	        c.output(row);
////	      }
////	    }
////	    
////	  }
//	    
////	}
//	  