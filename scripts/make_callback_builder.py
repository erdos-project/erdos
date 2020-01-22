import fire
import itertools

rust_imports = """// # Callback Builders for Multiple Streams
//
// This file was generated by with `scripts/make_callback_builder.sh {num_rs} {num_ws}`.
// To change callback builders, please edit `scripts/make_callback_builder.py`
// and use `scripts/make_callback_builder.sh` to generate a new `callback_builder.rs` file.

use std::{{cell::RefCell, rc::Rc}};
use std::sync::Arc;
use std::borrow::Borrow;

use crate::{{
    dataflow::{{
        stream::{{InternalStatefulReadStream, StreamId}},
        Data, State, StatefulReadStream, Timestamp, WriteStream,
    }},
    node::operator_event::OperatorEvent,
}};

pub trait MultiStreamEventMaker {{
    fn receive_watermark(&mut self, stream_id: StreamId, t: Timestamp) -> Vec<OperatorEvent>;
}}
"""

num_to_str = {
    0: "Zero",
    1: "One",
    2: "Two",
    3: "Three",
    4: "Four",
    5: "Five",
    6: "Six",
    7: "Seven",
    8: "Eight",
}


def make_struct_name(num_rs, num_ws, has_state):
    return "{}Read{}Write{}".format(num_to_str[num_rs], num_to_str[num_ws],
                                    "Stateful" if has_state else "")


def make_type_params(num_rs, num_ws, has_state, include_traits=False):
    make_read_stream_type = ((lambda x: "S{}: State".format(x)) if
                             include_traits else (lambda x: "S{}".format(x)))
    make_write_stream_type = ((lambda y: "W{}: Data".format(y)) if
                              include_traits else (lambda y: "W{}".format(y)))
    return ", ".join(
        itertools.chain(
            map(make_read_stream_type, range(num_rs)),
            map(make_write_stream_type, range(num_ws)),
            ["S: State" if include_traits else "S"] if has_state else []))


def make_callback_type(num_rs, num_ws, has_state):
    types = ", ".join(
        itertools.chain(
            map(lambda x: "&S{}".format(x), range(num_rs)),
            map(lambda y: "&mut WriteStream<W{}>".format(y), range(num_ws))))
    return "Fn({}{})".format(
        '&Timestamp, &mut S, ' if has_state else '&Timestamp, ', types)


def make_constructor_args(num_rs, num_ws, has_state):
    return ", ".join(
        itertools.chain(
            map(
                lambda x: "rs{}: &StatefulReadStream<R{}, S{}>".format(
                    x, x, x), range(num_rs)),
            map(lambda y: "ws{}: WriteStream<W{}>".format(y, y),
                range(num_ws)), ["state: S"] if has_state else []))


def make_read_stream_id_declarations(num_rs, indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda x: "rs{}_id: StreamId,".format(x), range(num_rs)))


def make_read_stream_state_declarations(num_rs, indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda x: "rs{}_state: Arc<S{}>,".format(x, x), range(num_rs)))


def make_write_stream_declarations(num_ws, indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda y: "ws{}: WriteStream<W{}>,".format(y, y), range(num_ws)))


def make_read_stream_id_assignments(num_rs,
                                    template="rs{x}.get_id()",
                                    indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda x: "rs{}_id: {},".format(x, template.format(x=x)),
            range(num_rs)))


def make_read_stream_state_assignments(num_rs,
                                       template="rs{x}.get_state()",
                                       indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda x: "rs{}_state: {},".format(x, template.format(x=x)),
            range(num_rs)))


def make_write_stream_assignments(num_ws,
                                  var_template="ws{y}",
                                  indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda y: "ws{}: {}.clone(),".format(y, var_template.format(y=y)),
            range(num_ws)))


def make_received_watermark_declarations(num_rs, indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda x: "rs{}_watermark: Option<Timestamp>,".format(x),
            range(num_rs)))


def make_received_watermark_assignments(num_rs, indent_level=1):
    indent = " " * (4 * indent_level)
    return "\n{}".format(indent).join(
        map(lambda x: "rs{}_watermark: None,".format(x), range(num_rs)))


add_state_template = """
    pub fn add_state<S: State>(&mut self, state: S) -> Rc<RefCell<{name}<{type_params}>>> {{
        let result = Rc::new(RefCell::new({name} {{
            children: Vec::new(),
            watermark_callbacks: Vec::new(),
            {read_stream_id_assignments}
            {read_stream_state_assignments}
            {write_stream_assignments}
            {received_watermark_assignments}
            state: Arc::new(state),
        }}));
        self.children.push(Rc::clone(&result) as Rc<RefCell<dyn MultiStreamEventMaker>>);
        result
    }}"""


def make_add_state(num_rs, num_ws):
    struct_name = make_struct_name(num_rs, num_ws, True)
    type_params = make_type_params(num_rs, num_ws, True, False)
    read_stream_id_assignments = make_read_stream_id_assignments(
        num_rs, "self.rs{x}_id", 3)
    read_stream_state_assignments = make_read_stream_state_assignments(
        num_rs, "Arc::clone(&self.rs{x}_state)", 3)
    write_stream_assignments = make_write_stream_assignments(
        num_ws, "self.ws{y}", 3)
    received_watermark_assignments = make_received_watermark_assignments(
        num_rs, 3)
    return add_state_template.format(
        name=struct_name,
        type_params=type_params,
        read_stream_id_assignments=read_stream_id_assignments,
        read_stream_state_assignments=read_stream_state_assignments,
        write_stream_assignments=write_stream_assignments,
        received_watermark_assignments=received_watermark_assignments)


add_read_stream_template = """
    pub fn add_read_stream<R{num_rs}: Data, S{num_rs}: 'static + State>(&mut self, read_stream: &StatefulReadStream<R{num_rs}, S{num_rs}>) -> Rc<RefCell<{name}<{type_params}>>> {{
        let result = Rc::new(RefCell::new({name} {{
            children: Vec::new(),
            watermark_callbacks: Vec::new(),
            {read_stream_id_assignments}
            rs{num_rs}_id: read_stream.get_id(),
            {read_stream_state_assignments}
            rs{num_rs}_state: read_stream.get_state(),
            {write_stream_assignments}
            {received_watermark_assignments}
        }}));
        self.children.push(Rc::clone(&result) as Rc<RefCell<dyn MultiStreamEventMaker>>);

        // Register child with read stream
        let internal_read_stream: Rc<RefCell<InternalStatefulReadStream<R{num_rs}, S{num_rs}>>> = read_stream.into();
        RefCell::borrow(&internal_read_stream).add_child(Rc::clone(&result));

        result
    }}"""


def make_add_read_stream(num_rs, num_ws):
    struct_name = make_struct_name(num_rs + 1, num_ws, False)
    type_params = make_type_params(num_rs + 1, num_ws, False)
    read_stream_id_assignments = make_read_stream_id_assignments(
        num_rs, "self.rs{x}_id", 3)
    read_stream_state_assignments = make_read_stream_state_assignments(
        num_rs, "Arc::clone(&self.rs{x}_state)", 3)
    write_stream_assignments = make_write_stream_assignments(
        num_ws, "self.ws{y}", 3)
    received_watermark_assignments = make_received_watermark_assignments(
        num_rs + 1, 3)
    return add_read_stream_template.format(
        name=struct_name,
        type_params=type_params,
        read_stream_id_assignments=read_stream_id_assignments,
        read_stream_state_assignments=read_stream_state_assignments,
        write_stream_assignments=write_stream_assignments,
        received_watermark_assignments=received_watermark_assignments,
        num_rs=num_rs)


add_write_stream_template = """
    pub fn add_write_stream<W{num_ws}: Data>(&mut self, write_stream: &WriteStream<W{num_ws}>) -> Rc<RefCell<{name}<{type_params}>>> {{
        let result = Rc::new(RefCell::new({name} {{
            children: Vec::new(),
            watermark_callbacks: Vec::new(),
            {read_stream_id_assignments}
            {read_stream_state_assignments}
            {write_stream_assignments}
            {received_watermark_assignments}
            ws{num_ws}: write_stream.clone(),
        }}));
        self.children.push(Rc::clone(&result) as Rc<RefCell<dyn MultiStreamEventMaker>>);
        result
    }}"""


def make_add_write_stream(num_rs, num_ws):
    struct_name = make_struct_name(num_rs, num_ws + 1, False)
    type_params = make_type_params(num_rs, num_ws + 1, False)
    read_stream_id_assignments = make_read_stream_id_assignments(
        num_rs, "self.rs{x}_id", 3)
    read_stream_state_assignments = make_read_stream_state_assignments(
        num_rs, "Arc::clone(&self.rs{x}_state)", 3)
    write_stream_assignments = make_write_stream_assignments(
        num_ws, "self.ws{y}", 3)
    received_watermark_assignments = make_received_watermark_assignments(
        num_rs, 3)
    return add_write_stream_template.format(
        name=struct_name,
        type_params=type_params,
        read_stream_id_assignments=read_stream_id_assignments,
        read_stream_state_assignments=read_stream_state_assignments,
        write_stream_assignments=write_stream_assignments,
        received_watermark_assignments=received_watermark_assignments,
        num_ws=num_ws)


make_receive_watermark_template = """
    fn receive_watermark(&mut self, stream_id: StreamId, t: Timestamp) -> Vec<OperatorEvent> {{
        let some_t = Some(t.clone());

        {set_watermark}
        
        let child_events: Vec<OperatorEvent> = self.children.iter()
            .map(|child| child.borrow_mut().receive_watermark(stream_id, t.clone()))
            .flatten()
            .collect();

        let mut events = Vec::new();
        if {is_low_watermark} && !(child_events.is_empty() && self.watermark_callbacks.is_empty()) {{
            let watermark_callbacks = self.watermark_callbacks.clone();
            {clone_state}
            {get_states}
            {clone_write_streams}
            if !watermark_callbacks.is_empty() || !child_events.is_empty() {{
                events.push(OperatorEvent::new(t.clone(), true, move || {{
                    for callback in watermark_callbacks {{
                        (callback)({args});
                    }}
                    for event in child_events {{
                        (event.callback)();
                    }}
                }}));
            }}
        }}

        events
    }}
"""


def make_receive_watermark(num_rs, num_ws, has_state):
    is_low_watermark = " && ".join(
        map(lambda x: "some_t <= self.rs{}_watermark".format(x),
            range(num_rs)))
    reset_watermarks = "\n".join(
        map(lambda x: "self.rs{}_watermark = false;".format(x), range(num_rs)))
    get_states = "\n".join(
        map(
            lambda x: "let rs{}_state = Arc::clone(&self.rs{}_state);".format(
                x, x), range(num_rs)))
    clone_write_streams = "\n".join(
        map(lambda y: "let mut ws{} = self.ws{}.clone();".format(y, y),
            range(num_ws)))
    clone_state = "let mut state = Arc::clone(&self.state);" if has_state else ""
    args = ", ".join(
        itertools.chain(["&t"],
                        ["unsafe { Arc::get_mut_unchecked(&mut state) }"]
                        if has_state else [],
                        map(lambda x: "&rs{}_state.borrow()".format(x),
                            range(num_rs)),
                        map(lambda y: "&mut ws{}".format(y), range(num_ws))))
    set_watermark = " else ".join(
        map(
            lambda x: """if stream_id == self.rs{}_id {{
            if some_t > self.rs{}_watermark {{
                self.rs{}_watermark = Some(t.clone());
            }} else {{
                // The watermark is outdated
                return Vec::new();
            }}
        }}""".format(x, x, x), range(num_rs)))
    return make_receive_watermark_template.format(
        is_low_watermark=is_low_watermark,
        reset_watermarks=reset_watermarks,
        clone_state=clone_state,
        get_states=get_states,
        clone_write_streams=clone_write_streams,
        set_watermark=set_watermark,
        args=args)


builder_template = """
pub struct {name}<{types}> {{
    children: Vec<Rc<RefCell<dyn MultiStreamEventMaker>>>,
    watermark_callbacks: Vec<Arc<dyn {callback_type}>>,
    {read_stream_id_declarations}
    {read_stream_state_declarations}
    {write_stream_declarations}
    {received_watermark_declarations}
    {state_declaration}
}}

impl<{types}> {name}<{type_params}> {{
    pub fn new<{rs_type_params}>({constructor_args}) -> Self {{
        Self {{
            children: Vec::new(),
            watermark_callbacks: Vec::new(),
            {read_stream_id_assignments}
            {read_stream_state_assignments}
            {write_stream_assignments}
            {received_watermark_assignments}
            {state_assignment}
        }}
    }}

    pub fn add_watermark_callback<F: 'static + {callback_type}>(&mut self, callback: F) {{
        self.watermark_callbacks.push(Arc::new(callback));
    }}

    {add_state}
    {add_read_stream}
    {add_write_stream}
}}

impl<{types}> MultiStreamEventMaker for {name}<{type_params}> {{
    {make_events}
}}
"""


def make_builder(num_rs,
                 num_ws,
                 has_state,
                 include_add_read_stream=True,
                 include_add_write_stream=True):
    struct_name = make_struct_name(num_rs, num_ws, has_state)
    types = make_type_params(num_rs, num_ws, has_state, True)
    type_params = make_type_params(num_rs, num_ws, has_state, False)
    rs_type_params = ", ".join(
        map(lambda x: "R{}: Data".format(x), range(num_rs)))
    callback_type = make_callback_type(num_rs, num_ws, has_state)
    read_stream_id_declarations = make_read_stream_id_declarations(num_rs, 1)
    read_stream_state_declarations = make_read_stream_state_declarations(
        num_rs, 1)
    write_stream_declarations = make_write_stream_declarations(num_ws, 1)
    received_watermark_declarations = make_received_watermark_declarations(
        num_rs, 1)
    state_declaration = "state: Arc<S>," if has_state else ""
    add_state_str = make_add_state(num_rs, num_ws) if not has_state else ""
    add_read_stream = make_add_read_stream(
        num_rs, num_ws) if include_add_read_stream and not has_state else ""
    add_write_stream = make_add_write_stream(
        num_rs, num_ws) if include_add_write_stream and not has_state else ""
    make_events = make_receive_watermark(num_rs, num_ws, has_state)
    constructor_args = make_constructor_args(num_rs, num_ws, has_state)
    read_stream_id_assignments = make_read_stream_id_assignments(
        num_rs, "rs{x}.get_id()", 4)
    read_stream_state_assignments = make_read_stream_state_assignments(
        num_rs, "rs{x}.get_state()", 4)
    write_stream_assignments = make_write_stream_assignments(
        num_ws, "ws{y}", 4)
    state_assignment = "state: Arc::new(state)," if has_state else ""
    received_watermark_assignments = make_received_watermark_assignments(
        num_rs, 4)
    return builder_template.format(
        add_state=add_state_str,
        add_read_stream=add_read_stream,
        add_write_stream=add_write_stream,
        make_events=make_events,
        name=struct_name,
        types=types,
        rs_type_params=rs_type_params,
        callback_type=callback_type,
        read_stream_id_declarations=read_stream_id_declarations,
        read_stream_state_declarations=read_stream_state_declarations,
        write_stream_declarations=write_stream_declarations,
        received_watermark_declarations=received_watermark_declarations,
        state_declaration=state_declaration,
        type_params=type_params,
        has_state=has_state,
        constructor_args=constructor_args,
        read_stream_id_assignments=read_stream_id_assignments,
        read_stream_state_assignments=read_stream_state_assignments,
        write_stream_assignments=write_stream_assignments,
        state_assignment=state_assignment,
        received_watermark_assignments=received_watermark_assignments)


class CLI(object):
    def build(self, num_rs, num_ws):
        print(rust_imports.format(num_rs=num_rs, num_ws=num_ws))
        for i in range(1, num_rs + 1):
            for j in range(num_ws + 1):
                if i == 1 and j == 0:
                    continue
                include_add_read_stream = (i < num_rs)
                include_add_write_stream = (j < num_ws)
                print(
                    make_builder(
                        i,
                        j,
                        False,
                        include_add_read_stream=include_add_read_stream,
                        include_add_write_stream=include_add_write_stream))
                print(
                    make_builder(
                        i,
                        j,
                        True,
                        include_add_read_stream=include_add_read_stream,
                        include_add_write_stream=include_add_write_stream))


if __name__ == "__main__":
    fire.Fire(CLI)
