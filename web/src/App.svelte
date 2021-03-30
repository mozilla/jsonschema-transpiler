<script>
  import wasm from "../../Cargo.toml";
  import { onMount } from "svelte";

  let jst;
  let input = JSON.stringify(
    {
      items: {
        type: "integer",
      },
      type: "array",
    },
    " ",
    2
  );
  onMount(async () => {
    jst = await wasm();
    console.log(jst);
  });

  let output;
  let error;

  function convert(input) {
    try {
      output = jst.convert_bigquery_js(JSON.parse(input));
      error = null;
    } catch (err) {
      error = err;
    }
    return output;
  }

  $: jst && input && convert(input);
</script>

<style>
  main {
    max-width: 900px;
    margin: 0 auto;
    box-sizing: border-box;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
      Oxygen-Sans, Ubuntu, Cantarell, "Helvetica Neue", sans-serif;
  }
  textarea {
    width: 100%;
    height: 100px;
  }
</style>

<main>
  <h1>jsonschema-transpiler</h1>
  <textarea bind:value={input} />
  {#if error}
    {error}
  {:else}<textarea>{JSON.stringify(output, '', 2)}</textarea>{/if}
</main>
