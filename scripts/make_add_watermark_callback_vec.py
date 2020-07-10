import argparse

add_read_stream_template = """
    let bundle = match read_streams.get({i}) {{
        Some(rs) => bundle.add_read_stream(rs),
        None => {{
            {add_write_streams}
            return;
        }}
    }};
    let mut bundle = bundle.borrow_mut();
"""

add_write_stream_template = """
            let bundle = match write_streams.get({j}) {{
                Some(ws) => bundle.add_write_stream(ws),
                None => {{
                    {add_watermark_callback}
                    return;
                }}
            }};
            let mut bundle = bundle.borrow_mut();
"""

add_watermark_callback_template = """
                    bundle.add_watermark_callback_with_priority(move |t, {states}, {write_streams}| {{
                        let mut write_streams = vec![{write_streams_cloned}];
                        callback(t, &mut write_streams);
                    }}, priority);
"""


def make_add_watermark_callback_vec(num_read_streams, num_write_streams):
    result = "{"
    for i in range(1, num_read_streams):
        add_write_streams = ""
        for j in range(num_write_streams):
            states = ", ".join(["_" for _ in range(i)])
            write_streams = ", ".join(["ws{}".format(x) for x in range(j)])
            write_streams_cloned = ", ".join(
                ["ws{}.clone()".format(x) for x in range(j)])
            add_watermark_callback = add_watermark_callback_template.format(
                states=states,
                write_streams=write_streams,
                write_streams_cloned=write_streams_cloned)
            add_write_streams += add_write_stream_template.format(
                j=j, add_watermark_callback=add_watermark_callback)

        result += add_read_stream_template.format(
            i=i, add_write_streams=add_write_streams)
    result += "}"
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=
        "Generate callback builders for m read streams and n write streams.")
    parser.add_argument("read_streams",
                        type=int,
                        help="number of read streams")
    parser.add_argument("write_streams",
                        type=int,
                        help="number of write streams")

    args = parser.parse_args()

    print(
        make_add_watermark_callback_vec(args.read_streams, args.write_streams))
