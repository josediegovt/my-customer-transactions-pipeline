{% macro create_safe_to_date() %}
CREATE OR REPLACE FUNCTION public.safe_to_date(p_text text, p_fmt text)
RETURNS date
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN to_date(p_text, p_fmt);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;
{% endmacro %}

{% macro safe_to_date(date_expr, date_format) %}
public.safe_to_date({{ date_expr }}, '{{ date_format }}')
{% endmacro %}
