public class TotallyOrderedMulticast {

    public static void main(String[] args) throws InterruptedException {

        /* Reading input :
         * arg[0] - no. of processes,
         * arg[1] - total no. of send events/messages,
         * arg[2] - current pid
         */
        LamportLogicalClock totalOrder = new LamportLogicalClock(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2]);
        totalOrder.init();

        try {

            // Handle processes' connection concurrently
            if(LamportLogicalClock.connectionThread != null){
                LamportLogicalClock.connectionThread.join();
            }

            // Enforce total order concurrently
            if(LamportLogicalClock.orderThread != null){
                LamportLogicalClock.orderThread.join();
            }

            // Handle acknowledgments concurrently
            if(LamportLogicalClock.ackThread != null){
                LamportLogicalClock.ackThread.join();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Process "+ LamportLogicalClock.process_id + " ended!");

    }

}

