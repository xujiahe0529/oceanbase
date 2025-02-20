struct thread_node_t
{
  link_t link;
  pthread_t pd;
};

static link_t* global_thread_list = NULL;
static void thread_counter_reg()
{
  static __thread struct thread_node_t thread_node;
  thread_node.pd = pthread_self();
  thread_node.link.next = NULL;
  link_t* head = TAS(&global_thread_list, &thread_node.link);
  thread_node.link.next = head;
}

static int64_t thread_counter_sum(int64_t* addr)
{
  int64_t s = 0;
  uint64_t offset = (uint64_t)addr - (uint64_t)pthread_self();
  for(link_t* p = global_thread_list; p; p = p->next) {
    struct thread_node_t* node = structof(p, struct thread_node_t, link);
    s += *(int64_t*)((uint64_t)node->pd + offset);
  }
  return s;
}
